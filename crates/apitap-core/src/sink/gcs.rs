//! Google Cloud Storage destination:
//! `gcs://bucket[/prefix]?format=csv|parquet[&credentials=/path/key.json]`.
//!
//! A table lands as FILES: `format=csv` (default) writes ONE gzipped CSV object
//! (`<prefix><table>.csv.gz`, header row included) — workers stream their own
//! staging parts and finalize COMPOSES them server-side, so the final object
//! appears atomically or not at all. `format=parquet` writes a directory of
//! part files (`<prefix><table>/part-NNNNN.parquet`, ZSTD, one per worker) —
//! the columnar convention every reader expects; parts can't be concatenated,
//! so the directory swap is per-object (each rewrite is atomic, the set is
//! not — documented).
//!
//! Both lanes reuse proven machinery: the CSV bytes come from the same
//! TSV→CSV transcoder as the BigQuery load lane ([`crate::wire::csvout`]),
//! the Parquet bytes from the same encoder ([`crate::wire::bqparquet`]), and
//! auth is the shared service-account flow ([`crate::gcp`]) with the storage
//! read-write scope. Uploads stream through GCS resumable sessions in
//! 256 KiB-aligned chunks — file size never bounds memory.
//!
//! Incremental modes are refused (objects have no upsert) — `mode="replace"`.

use crate::error::{Error, Result};
use crate::plan::{Delivered, Lane, TablePlan, WireFormat};
use crate::wire::bqparquet::{parquet_col_ok, ParquetEncoder};
use crate::wire::csvout::{csv_quote_into, tsv_to_csv};
use crate::Mode;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const API: &str = "https://storage.googleapis.com/storage/v1";
const UPLOAD: &str = "https://storage.googleapis.com/upload/storage/v1";
/// Resumable-chunk alignment GCS requires for every non-final chunk.
const UPLOAD_ALIGN: usize = 256 * 1024;
/// Buffered bytes that trigger an upload chunk.
const SEND_THRESHOLD: usize = 8 * 1024 * 1024;
/// GCS compose caps at 32 source objects (header + ≤31 parts; pipe profiles
/// top out well under this).
const COMPOSE_MAX: usize = 32;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum GcsFormat {
    Csv,
    Parquet,
}

/// The once-per-destination resources: HTTP client + auth + layout.
#[derive(Clone)]
pub(crate) struct GcsConn {
    client: reqwest::Client,
    /// Kept to RE-MINT the ~1h token mid-transfer — a long upload (or a big
    /// multi-table run sharing one conn) must not die 401 at the finish line.
    credentials: Arc<String>,
    token: Arc<std::sync::Mutex<(String, std::time::Instant)>>,
    bucket: String,
    /// Normalized to end with '/' when non-empty.
    prefix: String,
    format: GcsFormat,
}

/// RFC 3986 percent-encode for one URL component ('/' included — object names
/// ride as a single path component in the JSON API).
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Decode one percent-encoded path segment from the user's URL (Url::path()
/// returns it ENCODED; without this a prefix with a space double-encodes).
fn decode_component(s: &str) -> Result<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = b
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| {
                    Error::InvalidInput(format!("gcs url: invalid percent-escape in '{s}'"))
                })?;
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|e| Error::InvalidInput(format!("gcs url path is not UTF-8: {e}")))
}

impl GcsConn {
    pub(crate) async fn parse(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url).map_err(|e| Error::InvalidInput(format!("gcs url: {e}")))?;
        let bucket = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "gcs url needs a bucket: gcs://<bucket>[/prefix]?format=csv|parquet\
                     [&credentials=/path/key.json]"
                        .into(),
                )
            })?
            .to_string();
        let mut prefix = u
            .path()
            .trim_matches('/')
            .split('/')
            .map(decode_component)
            .collect::<Result<Vec<_>>>()?
            .join("/");
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let mut credentials_path = None;
        let mut format = GcsFormat::Csv;
        // Raw pairs, percent-decoded WITHOUT '+'-as-space (query_pairs is form
        // decoding — it would mangle a key path containing a literal '+').
        for pair in u.query().unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let (k, v) = (decode_component(k)?, decode_component(v)?);
            match k.as_str() {
                "credentials" => credentials_path = Some(v),
                "format" => {
                    format = match v.as_str() {
                        "csv" => GcsFormat::Csv,
                        "parquet" => GcsFormat::Parquet,
                        other => {
                            return Err(Error::InvalidInput(format!(
                                "gcs url: unknown format '{other}' (supported: csv, parquet)"
                            )))
                        }
                    }
                }
                other => {
                    return Err(Error::InvalidInput(format!(
                        "unknown gcs url parameter '{other}' (supported: format, credentials)"
                    )))
                }
            }
        }
        let credentials = crate::gcp::read_credentials(credentials_path, "gcs")?;
        let client = reqwest::Client::new();
        let token = crate::gcp::fetch_access_token(&client, &credentials, SCOPE).await?;
        Ok(Self {
            client,
            credentials: Arc::new(credentials),
            token: Arc::new(std::sync::Mutex::new((token, std::time::Instant::now()))),
            bucket,
            prefix,
            format,
        })
    }

    /// The current bearer token, re-minted past 55 minutes (tokens live ~1h).
    /// Two racing refreshes just mint twice — harmless.
    async fn bearer(&self) -> Result<String> {
        {
            let t = self.token.lock().expect("gcs token");
            if t.1.elapsed() < std::time::Duration::from_secs(55 * 60) {
                return Ok(t.0.clone());
            }
        }
        let fresh = crate::gcp::fetch_access_token(&self.client, &self.credentials, SCOPE).await?;
        let mut t = self.token.lock().expect("gcs token");
        *t = (fresh.clone(), std::time::Instant::now());
        Ok(fresh)
    }

    /// Send with up to 3 attempts: transport errors and 5xx retry with backoff
    /// (every call here is idempotent — range re-PUTs included, per the
    /// resumable protocol). 4xx and success return immediately.
    async fn send_retry(
        &self,
        build: impl Fn() -> reqwest::RequestBuilder,
        what: &str,
    ) -> Result<reqwest::Response> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match build().send().await {
                Ok(resp) if resp.status().is_server_error() && attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                }
                Ok(resp) => return Ok(resp),
                Err(_) if attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                }
                Err(e) => return Err(Error::Transfer(format!("gcs {what}: {e}"))),
            }
        }
    }

    async fn check(resp: reqwest::Response, what: &str) -> Result<reqwest::Response> {
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Transfer(format!(
                "gcs {what} ({status}): {}",
                body.chars().take(400).collect::<String>().trim()
            )));
        }
        Ok(resp)
    }

    async fn simple_upload(&self, object: &str, bytes: Vec<u8>) -> Result<()> {
        let url = format!(
            "{UPLOAD}/b/{}/o?uploadType=media&name={}",
            self.bucket,
            enc(object)
        );
        let tok = self.bearer().await?;
        let resp = self
            .send_retry(
                || {
                    self.client
                        .post(&url)
                        .bearer_auth(&tok)
                        .header("content-type", "application/octet-stream")
                        .body(bytes.clone())
                },
                "upload",
            )
            .await?;
        Self::check(resp, "upload").await.map(|_| ())
    }

    async fn begin_resumable(&self, object: &str) -> Result<String> {
        let url = format!(
            "{UPLOAD}/b/{}/o?uploadType=resumable&name={}",
            self.bucket,
            enc(object)
        );
        let tok = self.bearer().await?;
        let resp = self
            .send_retry(
                || {
                    self.client
                        .post(&url)
                        .bearer_auth(&tok)
                        .header("content-length", "0")
                },
                "resumable begin",
            )
            .await?;
        let resp = Self::check(resp, "resumable begin").await?;
        resp.headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| Error::Transfer("gcs resumable begin: no session Location".into()))
    }

    /// One resumable PUT. Non-final chunks must be 256 KiB-aligned; `total`
    /// marks the final chunk. 308 = accepted-incomplete, 2xx = object done.
    async fn put_chunk(
        &self,
        session: &str,
        start: u64,
        bytes: Vec<u8>,
        total: Option<u64>,
    ) -> Result<()> {
        let end = start + bytes.len() as u64;
        let range = match (bytes.is_empty(), total) {
            (true, Some(t)) => format!("bytes */{t}"),
            (false, Some(t)) => format!("bytes {start}-{}/{t}", end - 1),
            (false, None) => format!("bytes {start}-{}/*", end - 1),
            (true, None) => return Ok(()),
        };
        let resp = self
            .send_retry(
                || {
                    self.client
                        .put(session)
                        .header("content-range", range.clone())
                        .body(bytes.clone())
                },
                "chunk upload",
            )
            .await?;
        let status = resp.status();
        // The status CONTRACT, not just "no error": a non-final chunk must get
        // 308 with the full range confirmed (GCS may accept FEWER bytes and
        // still answer 308); the FINAL chunk must get 2xx — a final 308 means
        // the object was never finalized, and treating it as success silently
        // drops this worker's whole part from the composed result.
        match (status.as_u16(), total) {
            (308, None) => {
                let confirmed = resp
                    .headers()
                    .get("range")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|r| r.rsplit('-').next())
                    .and_then(|n| n.parse::<u64>().ok());
                if confirmed == Some(end - 1) {
                    Ok(())
                } else {
                    Err(Error::Transfer(format!(
                        "gcs chunk upload: 308 confirmed bytes up to {confirmed:?}, \
                         expected {} — resumable session out of sync",
                        end - 1
                    )))
                }
            }
            (_, Some(_)) if status.is_success() => Ok(()),
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Transfer(format!(
                    "gcs chunk upload ({status}, final={}): {}",
                    total.is_some(),
                    body.chars().take(400).collect::<String>().trim()
                )))
            }
        }
    }

    /// Cancel a resumable session so GCS discards the partial object.
    async fn cancel(&self, session: &str) {
        let _ = self.client.delete(session).send().await;
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = format!(
                "{API}/b/{}/o?prefix={}&fields=items(name),nextPageToken",
                self.bucket,
                enc(prefix)
            );
            if let Some(t) = &token {
                url.push_str(&format!("&pageToken={}", enc(t)));
            }
            let tok = self.bearer().await?;
            let resp = self
                .send_retry(|| self.client.get(&url).bearer_auth(&tok), "list")
                .await?;
            let v: serde_json::Value = Self::check(resp, "list")
                .await?
                .json()
                .await
                .map_err(|e| Error::Transfer(format!("gcs list response: {e}")))?;
            for item in v["items"].as_array().unwrap_or(&Vec::new()) {
                if let Some(n) = item["name"].as_str() {
                    out.push(n.to_string());
                }
            }
            match v["nextPageToken"].as_str() {
                Some(t) => token = Some(t.to_string()),
                None => return Ok(out),
            }
        }
    }

    async fn delete(&self, object: &str) -> Result<()> {
        let url = format!("{API}/b/{}/o/{}", self.bucket, enc(object));
        let tok = self.bearer().await?;
        let resp = self
            .send_retry(|| self.client.delete(&url).bearer_auth(&tok), "delete")
            .await?;
        // 404 tolerated: sweeps race their own prior runs.
        if resp.status().is_success() || resp.status().as_u16() == 404 {
            return Ok(());
        }
        Self::check(resp, "delete").await.map(|_| ())
    }

    /// Server-side concatenation — the final CSV object appears in ONE
    /// metadata operation (atomic visibility), no bytes re-uploaded.
    async fn compose(&self, sources: &[String], dest: &str) -> Result<()> {
        if sources.len() > COMPOSE_MAX {
            return Err(Error::Transfer(format!(
                "gcs compose: {} parts exceeds the {COMPOSE_MAX}-object limit — \
                 lower parallel",
                sources.len()
            )));
        }
        let url = format!("{API}/b/{}/o/{}/compose", self.bucket, enc(dest));
        let body = serde_json::json!({
            "sourceObjects": sources.iter().map(|s| serde_json::json!({"name": s})).collect::<Vec<_>>(),
            "destination": {"contentType": "application/gzip"},
        });
        let tok = self.bearer().await?;
        let resp = self
            .send_retry(
                || self.client.post(&url).bearer_auth(&tok).json(&body),
                "compose",
            )
            .await?;
        Self::check(resp, "compose").await.map(|_| ())
    }

    /// Server-side copy (rewrite loop — handles the token protocol for any size).
    async fn rewrite(&self, src: &str, dst: &str) -> Result<()> {
        let mut token: Option<String> = None;
        loop {
            let mut url = format!(
                "{API}/b/{}/o/{}/rewriteTo/b/{}/o/{}",
                self.bucket,
                enc(src),
                self.bucket,
                enc(dst)
            );
            if let Some(t) = &token {
                url.push_str(&format!("?rewriteToken={}", enc(t)));
            }
            let tok = self.bearer().await?;
            let resp = self
                .send_retry(
                    || {
                        self.client
                            .post(&url)
                            .bearer_auth(&tok)
                            .header("content-length", "0")
                    },
                    "rewrite",
                )
                .await?;
            let v: serde_json::Value = Self::check(resp, "rewrite")
                .await?
                .json()
                .await
                .map_err(|e| Error::Transfer(format!("gcs rewrite response: {e}")))?;
            if v["done"].as_bool() == Some(true) {
                return Ok(());
            }
            token = v["rewriteToken"].as_str().map(str::to_string);
            if token.is_none() {
                return Err(Error::Transfer(
                    "gcs rewrite: not done and no rewriteToken".into(),
                ));
            }
        }
    }
}

pub(crate) struct GcsSink {
    conn: GcsConn,
    /// The table's file name (schema qualifiers dropped by dispatch).
    bare: String,
    staging: String,
    names: Arc<Vec<String>>,
    delivered: Arc<Vec<Delivered>>,
    next_part: Arc<AtomicU64>,
}

impl GcsSink {
    pub(crate) fn bind(conn: GcsConn, dest_table: &str, parallel: usize) -> Result<Self> {
        // The CSV lane composes header + one part per pipe; GCS compose caps at
        // 32 sources. Refuse BEFORE uploading anything, not after.
        if conn.format == GcsFormat::Csv && parallel + 1 > COMPOSE_MAX {
            return Err(Error::InvalidInput(format!(
                "format=csv supports at most {} pipes (GCS composes header + one \
                 part per pipe, 32-object limit) — lower parallel or use \
                 format=parquet",
                COMPOSE_MAX - 1
            )));
        }
        let bare = dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t);
        let staging = format!("{}{bare}__apitap_staging/", conn.prefix);
        Ok(Self {
            conn,
            bare: bare.to_string(),
            staging,
            names: Arc::new(Vec::new()),
            delivered: Arc::new(Vec::new()),
            next_part: Arc::new(AtomicU64::new(0)),
        })
    }

    fn ext(&self) -> &'static str {
        match self.conn.format {
            GcsFormat::Csv => "csv.gz",
            GcsFormat::Parquet => "parquet",
        }
    }
}

impl crate::sink::Sink for GcsSink {
    type Loader = GcsLoader;

    fn accepts(&self) -> &[WireFormat] {
        match self.conn.format {
            // The format is the USER'S explicit choice — exactly one lane each,
            // no silent fallback to a different file format.
            GcsFormat::Csv => &[WireFormat::TabSeparated],
            GcsFormat::Parquet => &[WireFormat::PgCopyBinary],
        }
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        _mode: Mode,
    ) -> Result<()> {
        // A single-column table's NULL row would be a BLANK CSV line — which
        // every standard reader silently drops. No honest spelling exists
        // (quoted "" means empty string), so refuse instead of losing rows.
        if self.conn.format == GcsFormat::Csv
            && plan.cols.len() == 1
            && plan.cols[0].nullable
        {
            return Err(Error::InvalidInput(format!(
                "single nullable column '{}' can't round-trip through CSV (a NULL \
                 row is a blank line, dropped by CSV readers) — use format=parquet, \
                 or a NOT NULL source column",
                plan.cols[0].name
            )));
        }
        let mut delivered = Vec::new();
        let mut names = Vec::new();
        for (c, lc) in plan.cols.iter().zip(lane.cols.iter()) {
            if self.conn.format == GcsFormat::Parquet && !parquet_col_ok(&c.udt, c.precision) {
                return Err(Error::InvalidInput(format!(
                    "column {} has type {} — the parquet lane can't encode it; \
                     cast it in a source view (e.g. {}::text) or use format=csv",
                    c.name, c.udt, c.name
                )));
            }
            names.push(c.name.clone());
            delivered.push(lc.delivered.clone());
        }
        self.names = Arc::new(names);
        self.delivered = Arc::new(delivered);
        // Sweep stale staging parts (crashed runs included).
        for obj in self.conn.list(&self.staging).await? {
            self.conn.delete(&obj).await?;
        }
        if self.conn.format == GcsFormat::Csv {
            // The header is its own tiny gzip member, named to sort BEFORE the
            // part files so finalize's sorted compose puts it first —
            // concatenated gzip members are one valid gzip stream.
            let mut line = Vec::new();
            for (i, c) in plan.cols.iter().enumerate() {
                if i > 0 {
                    line.push(b',');
                }
                csv_quote_into(c.name.as_bytes(), &mut line);
            }
            line.push(b'\n');
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            let bytes = gz
                .write_all(&line)
                .and_then(|_| gz.finish())
                .map_err(|e| Error::Transfer(format!("gcs header gzip: {e}")))?;
            self.conn
                .simple_upload(&format!("{}00-header.csv.gz", self.staging), bytes)
                .await?;
        }
        Ok(())
    }

    async fn loader(&self) -> Result<GcsLoader> {
        let n = self.next_part.fetch_add(1, Ordering::Relaxed);
        let object = format!("{}part-{n:05}.{}", self.staging, self.ext());
        let session = self.conn.begin_resumable(&object).await?;
        let pq = match self.conn.format {
            GcsFormat::Parquet => Some(ParquetEncoder::new(
                self.names.as_ref().clone(),
                self.delivered.as_ref().clone(),
                None,
            )?),
            GcsFormat::Csv => None,
        };
        Ok(GcsLoader {
            conn: self.conn.clone(),
            session,
            delivered: self.delivered.clone(),
            pq,
            gz: match self.conn.format {
                GcsFormat::Csv => Some(flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::fast(),
                )),
                GcsFormat::Parquet => None,
            },
            csv: Vec::new(),
            scratch: Vec::new(),
            wm: None,
            offset: 0,
            rows: 0,
        })
    }

    async fn rows_staged(&self, loaded: u64) -> Result<u64> {
        Ok(loaded)
    }

    async fn finalize(&self, rows: u64, _mode: Mode) -> Result<()> {
        let mut parts = self.conn.list(&self.staging).await?;
        parts.sort();
        if rows == 0 {
            // 0-row guard: destination untouched, staging swept.
            for p in &parts {
                self.conn.delete(p).await?;
            }
            return Ok(());
        }
        // Once the data has LANDED, cleanup failures must not fail the run —
        // a reported failure with a fully-landed destination is a lie, and the
        // next prepare sweeps staging anyway. Hence best-effort deletes below.
        match self.conn.format {
            GcsFormat::Csv => {
                let dest = format!("{}{}.csv.gz", self.conn.prefix, self.bare);
                self.conn.compose(&parts, &dest).await?;
                for p in &parts {
                    let _ = self.conn.delete(p).await;
                }
                // A previous format=parquet run of this table would otherwise
                // stay live next to the new CSV forever.
                for old in self
                    .conn
                    .list(&format!("{}{}/", self.conn.prefix, self.bare))
                    .await
                    .unwrap_or_default()
                {
                    if is_part_object(&old) {
                        let _ = self.conn.delete(&old).await;
                    }
                }
            }
            GcsFormat::Parquet => {
                let dir = format!("{}{}/", self.conn.prefix, self.bare);
                let stale: Vec<String> = self.conn.list(&dir).await?;
                let mut fresh = Vec::with_capacity(parts.len());
                for (i, p) in parts.iter().enumerate() {
                    let dst = format!("{dir}part-{i:05}.parquet");
                    self.conn.rewrite(p, &dst).await?;
                    fresh.push(dst);
                }
                // Per-object renames aren't a transaction: readers can see a
                // mixed directory until this finishes (a failure here leaves
                // the mix until the next successful run — documented). Only
                // apitap's OWN part files are swept; anything else a user put
                // in the directory is not ours to delete.
                for s in stale {
                    if !fresh.contains(&s) && is_part_object(&s) {
                        let _ = self.conn.delete(&s).await;
                    }
                }
                for p in &parts {
                    let _ = self.conn.delete(p).await;
                }
                // A previous format=csv run of this table would otherwise stay
                // live next to the new parts forever.
                let _ = self
                    .conn
                    .delete(&format!("{}{}.csv.gz", self.conn.prefix, self.bare))
                    .await;
            }
        }
        Ok(())
    }
}

/// apitap's own part-file names (`part-NNNNN.parquet` under a table dir) —
/// the ONLY things a stale-sweep may delete.
fn is_part_object(name: &str) -> bool {
    let base = name.rsplit_once('/').map_or(name, |(_, b)| b);
    base.strip_prefix("part-")
        .and_then(|r| r.strip_suffix(".parquet"))
        .is_some_and(|digits| digits.len() == 5 && digits.bytes().all(|b| b.is_ascii_digit()))
}

pub(crate) struct GcsLoader {
    conn: GcsConn,
    session: String,
    delivered: Arc<Vec<Delivered>>,
    /// Parquet lane encoder; None = the CSV lane.
    pq: Option<ParquetEncoder>,
    /// CSV lane gzip stream; its inner Vec is the pending upload buffer.
    gz: Option<flate2::write::GzEncoder<Vec<u8>>>,
    csv: Vec<u8>,
    scratch: Vec<u8>,
    wm: Option<String>,
    offset: u64,
    rows: u64,
}

impl GcsLoader {
    /// Upload the aligned prefix of `pending`, leaving the remainder buffered.
    async fn drain(&mut self, pending: &mut Vec<u8>) -> Result<()> {
        let take = (pending.len() / UPLOAD_ALIGN) * UPLOAD_ALIGN;
        if take == 0 {
            return Ok(());
        }
        let chunk: Vec<u8> = pending.drain(..take).collect();
        self.conn
            .put_chunk(&self.session, self.offset, chunk, None)
            .await?;
        self.offset += take as u64;
        Ok(())
    }
}

impl crate::sink::Loader for GcsLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        match &mut self.pq {
            Some(pq) => {
                self.rows += pq.push(&buf)?;
                let mut pending = {
                    let mut b = pq.out.0.lock().expect("parquet buf");
                    if b.len() < SEND_THRESHOLD {
                        return Ok(());
                    }
                    std::mem::take(&mut *b)
                };
                self.drain(&mut pending).await?;
                let mut b = self
                    .pq
                    .as_mut()
                    .expect("pq lane")
                    .out
                    .0
                    .lock()
                    .expect("parquet buf");
                // The encoder may have appended while we uploaded — put the
                // unaligned remainder back IN FRONT.
                pending.extend_from_slice(&b);
                *b = pending;
            }
            None => {
                self.csv.clear();
                self.rows += tsv_to_csv(
                    &buf,
                    &self.delivered,
                    false,
                    None,
                    &mut self.wm,
                    &mut self.csv,
                    &mut self.scratch,
                )?;
                let gz = self.gz.as_mut().expect("csv lane");
                gz.write_all(&self.csv)
                    .map_err(|e| Error::Transfer(format!("gcs gzip: {e}")))?;
                if gz.get_ref().len() >= SEND_THRESHOLD {
                    let mut pending = std::mem::take(self.gz.as_mut().expect("csv lane").get_mut());
                    self.drain(&mut pending).await?;
                    *self.gz.as_mut().expect("csv lane").get_mut() = pending;
                }
            }
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<u64> {
        let mut pending = match (self.pq.take(), self.gz.take()) {
            (Some(mut pq), _) => {
                pq.finish_file()?;
                let mut b = pq.out.0.lock().expect("parquet buf");
                std::mem::take(&mut *b)
            }
            (None, Some(gz)) => gz
                .finish()
                .map_err(|e| Error::Transfer(format!("gcs gzip finish: {e}")))?,
            (None, None) => Vec::new(),
        };
        let total = self.offset + pending.len() as u64;
        let chunk = std::mem::take(&mut pending);
        self.conn
            .put_chunk(&self.session, self.offset, chunk, Some(total))
            .await?;
        Ok(self.rows)
    }

    async fn abort(self, cause: Error) -> Error {
        // Cancel the session so GCS discards the partial part object.
        self.conn.cancel(&self.session).await;
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enc_and_decode_component_roundtrip() {
        assert_eq!(enc("e2e/año 1.csv.gz"), "e2e%2Fa%C3%B1o%201.csv.gz");
        assert_eq!(decode_component("a%C3%B1o%201").unwrap(), "año 1");
        // '+' is a literal plus in a path, never a space.
        assert_eq!(decode_component("k+ey.json").unwrap(), "k+ey.json");
        assert!(decode_component("bad%zz").is_err());
    }

    #[test]
    fn stale_sweep_only_touches_our_part_files() {
        assert!(is_part_object("e2e/events/part-00003.parquet"));
        assert!(!is_part_object("e2e/events/part-3.parquet"));
        assert!(!is_part_object("e2e/events/users-notes.parquet"));
        assert!(!is_part_object("e2e/events/part-00003.csv"));
        assert!(!is_part_object("e2e/events/README.md"));
    }
}
