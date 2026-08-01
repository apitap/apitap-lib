//! S3-compatible object-store sink: `s3://<bucket>[/prefix]?format=parquet`
//! against AWS S3, MinIO, R2, and the S3-compatible object storage most
//! European clouds ship. Modeled on the GCS sink, with three deliberate
//! differences: SigV4 signing instead of a bearer token, S3 multipart upload
//! instead of resumable sessions (5 MiB minimum non-final part, XML
//! responses), and — v1 scope — the Parquet lane only: the CSV lane's
//! single-object guarantee rode on GCS `compose`, which S3 lacks; it returns
//! once an UploadPartCopy-based concat is built.
//!
//! Incremental modes are refused (objects have no upsert) — mode="replace".
//! Append lands with the Iceberg catalog destination, where append is a
//! snapshot commit rather than object mutation.

use crate::aws::{payload_hash, read_credentials, sigv4_headers, AwsCreds, UNSIGNED_PAYLOAD};
use crate::error::{Error, Result};
use crate::plan::{Delivered, Lane, TablePlan, WireFormat};
use crate::wire::bqparquet::{parquet_col_ok, ParquetEncoder};
use crate::Mode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// S3 requires every non-final multipart part to be ≥ 5 MiB.
const MIN_PART: usize = 5 * 1024 * 1024;
/// Buffered encoder output that triggers a part upload (comfortably over the
/// 5 MiB floor, matching the GCS sink's send threshold).
const SEND_THRESHOLD: usize = 8 * 1024 * 1024;
const _: () = assert!(SEND_THRESHOLD >= MIN_PART);

#[derive(Clone)]
pub(crate) struct S3Conn {
    client: reqwest::Client,
    creds: AwsCreds,
    bucket: String,
    /// Normalized to end with '/' when non-empty.
    prefix: String,
    region: String,
    /// Scheme+host[:port] the requests go to, no trailing slash.
    endpoint: String,
    /// Host header value (must match what's signed).
    host: String,
    /// Path-style (`/{bucket}/{key}`, MinIO et al.) vs virtual-host style.
    path_style: bool,
}

/// RFC 3986 percent-encode one path SEGMENT (no '/' — segments are joined by
/// the caller so the canonical URI keeps its separators).
fn enc_seg(s: &str) -> String {
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

/// Decode one percent-encoded component from the user's URL (same contract as
/// the GCS sink's decoder: '+' stays literal).
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
                    Error::InvalidInput(format!("s3 url: invalid percent-escape in '{s}'"))
                })?;
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| Error::InvalidInput(format!("s3 url path is not UTF-8: {e}")))
}

/// Minimal XML tag extraction — S3's response surface here is flat elements
/// with no attributes or nesting ambiguity, and the tree has no XML crate.
fn xml_tag(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

fn xml_tags(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(i) = rest.find(&open) {
        let start = i + open.len();
        let Some(j) = rest[start..].find(&close) else { break };
        out.push(rest[start..start + j].to_string());
        rest = &rest[start + j + close.len()..];
    }
    out
}

impl S3Conn {
    pub(crate) async fn parse(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url).map_err(|e| Error::InvalidInput(format!("s3 url: {e}")))?;
        let bucket = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "s3 url needs a bucket: s3://<bucket>[/prefix]?format=parquet\
                     [&endpoint=…][&region=…][&access_key_id=…&secret_access_key=…]"
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
        let (mut region, mut endpoint) = (None, None);
        let (mut key_id, mut secret, mut token) = (None, None, None);
        let mut format_seen = None;
        for pair in u.query().unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let (k, v) = (decode_component(k)?, decode_component(v)?);
            match k.as_str() {
                "format" => format_seen = Some(v),
                "region" => region = Some(v),
                "endpoint" => endpoint = Some(v),
                "access_key_id" => key_id = Some(v),
                "secret_access_key" => secret = Some(v),
                "session_token" => token = Some(v),
                other => {
                    return Err(Error::InvalidInput(format!(
                        "unknown s3 url parameter '{other}' (supported: format, region, \
                         endpoint, access_key_id, secret_access_key, session_token)"
                    )))
                }
            }
        }
        match format_seen.as_deref() {
            None | Some("parquet") => {}
            Some("csv") => {
                return Err(Error::InvalidInput(
                    "s3: format=csv is not supported yet (the single-object CSV \
                     guarantee needs an S3-side concat that is still on the \
                     roadmap) — use format=parquet"
                        .into(),
                ))
            }
            Some(other) => {
                return Err(Error::InvalidInput(format!(
                    "s3 url: unknown format '{other}' (supported: parquet)"
                )))
            }
        }
        let region = region
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        let creds = read_credentials(key_id, secret, token)?;
        // An explicit endpoint (MinIO/R2/…) means path-style addressing; bare
        // AWS gets virtual-host style, the only style S3 still promises.
        let (endpoint, host, path_style) = match endpoint {
            Some(e) => {
                let e = e.trim_end_matches('/').to_string();
                let host = e
                    .split_once("://")
                    .map(|(_, h)| h.to_string())
                    .ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "s3 endpoint must include a scheme, e.g. http://127.0.0.1:9000 — got '{e}'"
                        ))
                    })?;
                (e, host, true)
            }
            None => {
                let host = format!("{bucket}.s3.{region}.amazonaws.com");
                (format!("https://{host}"), host, false)
            }
        };
        Ok(Self {
            client: reqwest::Client::new(),
            creds,
            bucket,
            prefix,
            region,
            endpoint,
            host,
            path_style,
        })
    }

    /// Build a connection from resolved parts — the Iceberg sink lands data
    /// under a table LOCATION owned by the catalog (`s3://bucket/…`), so the
    /// bucket comes from that location, not from the user's URL.
    pub(crate) fn from_parts(
        bucket: String,
        endpoint: Option<String>,
        region: String,
        creds: AwsCreds,
    ) -> Result<Self> {
        let (endpoint, host, path_style) = match endpoint {
            Some(e) => {
                let e = e.trim_end_matches('/').to_string();
                let host = e
                    .split_once("://")
                    .map(|(_, h)| h.to_string())
                    .ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "s3 endpoint must include a scheme, e.g. http://127.0.0.1:9000 — got '{e}'"
                        ))
                    })?;
                (e, host, true)
            }
            None => {
                let host = format!("{bucket}.s3.{region}.amazonaws.com");
                (format!("https://{host}"), host, false)
            }
        };
        Ok(Self {
            client: reqwest::Client::new(),
            creds,
            bucket,
            prefix: String::new(),
            region,
            endpoint,
            host,
            path_style,
        })
    }

    /// Single-request PUT for small objects (manifests, delete files).
    pub(crate) async fn put_object(&self, object: &str, body: Vec<u8>) -> Result<()> {
        let uri = self.uri_for(object);
        let hash = payload_hash(&body);
        let r = self
            .request(reqwest::Method::PUT, &uri, &[], body, &hash, &[])
            .await?;
        Self::check(r, "put object").await.map(|_| ())
    }

    /// Whole-object GET (manifest lists are KBs; never used for data files).
    pub(crate) async fn get_object(&self, object: &str) -> Result<Vec<u8>> {
        let uri = self.uri_for(object);
        let r = self
            .request(reqwest::Method::GET, &uri, &[], Vec::new(), &payload_hash(b""), &[])
            .await?;
        let status = r.status();
        if !status.is_success() {
            let body = r.text().await.unwrap_or_default();
            return Err(Error::Transfer(format!("s3 get {object}: {status} {body:.200}")));
        }
        r.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::Transfer(format!("s3 get {object}: {e}")))
    }

    /// Ranged GET — the Iceberg sink reads parquet FOOTERS of committed data
    /// files to derive a bootstrap watermark from their column statistics.
    pub(crate) async fn get_range(&self, object: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        let uri = self.uri_for(object);
        let range = format!("bytes={start}-{}", start + len - 1);
        let mut last = None;
        for attempt in 1..=3u32 {
            let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            // `range` is a signed header here — it changes the response bytes.
            let headers = sigv4_headers(
                &self.creds,
                "GET",
                &self.host,
                &uri,
                &[],
                &self.region,
                &amz_date,
                &payload_hash(b""),
                &[("range", range.clone())],
            );
            let url = format!("{}{}", self.endpoint, uri);
            let mut req = self.client.get(&url);
            for (k, v) in &headers {
                req = req.header(k.as_str(), v);
            }
            match req.send().await {
                Ok(r) if r.status().is_success() => {
                    return r
                        .bytes()
                        .await
                        .map(|b| b.to_vec())
                        .map_err(|e| Error::Transfer(format!("s3 range get: {e}")));
                }
                Ok(r) if r.status().is_server_error() && attempt < 3 => {
                    last = Some(Error::Transfer(format!("s3 range get: {}", r.status())));
                }
                Ok(r) => {
                    return Err(Error::Transfer(format!("s3 range get {object}: {}", r.status())))
                }
                Err(e) if attempt < 3 => last = Some(Error::Transfer(format!("s3 range get: {e}"))),
                Err(e) => return Err(Error::Transfer(format!("s3 range get: {e}"))),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
        }
        Err(last.unwrap_or_else(|| Error::Transfer("s3 range get: retries exhausted".into())))
    }

    /// Canonical (signed) URI path for an object — encoded segments, kept '/'.
    fn uri_for(&self, object: &str) -> String {
        let key = object.split('/').map(enc_seg).collect::<Vec<_>>().join("/");
        if self.path_style {
            format!("/{}/{key}", enc_seg(&self.bucket))
        } else {
            format!("/{key}")
        }
    }

    /// Sign + send with retries. The signature binds the timestamp, so it is
    /// recomputed per attempt.
    async fn request(
        &self,
        method: reqwest::Method,
        object_uri: &str,
        query: &[(String, String)],
        body: Vec<u8>,
        content_sha256: &str,
        extra_amz: &[(&str, String)],
    ) -> Result<reqwest::Response> {
        let mut last = None;
        for attempt in 1..=3u32 {
            let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            let headers = sigv4_headers(
                &self.creds,
                method.as_str(),
                &self.host,
                object_uri,
                query,
                &self.region,
                &amz_date,
                content_sha256,
                extra_amz,
            );
            let qs = if query.is_empty() {
                String::new()
            } else {
                let parts: Vec<String> = query
                    .iter()
                    .map(|(k, v)| {
                        if v.is_empty() {
                            enc_seg(k)
                        } else {
                            format!("{}={}", enc_seg(k), enc_seg(v))
                        }
                    })
                    .collect();
                format!("?{}", parts.join("&"))
            };
            let url = format!("{}{}{qs}", self.endpoint, object_uri);
            let mut req = self.client.request(method.clone(), &url);
            for (k, v) in &headers {
                req = req.header(k.as_str(), v);
            }
            match req.body(body.clone()).send().await {
                Ok(r) if r.status().is_server_error() && attempt < 3 => {
                    last = Some(Error::Transfer(format!("s3 {method} {object_uri}: {}", r.status())));
                }
                Ok(r) => return Ok(r),
                Err(e) if attempt < 3 => last = Some(Error::Transfer(format!("s3 send: {e}"))),
                Err(e) => return Err(Error::Transfer(format!("s3 send: {e}"))),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
        }
        Err(last.unwrap_or_else(|| Error::Transfer("s3: retries exhausted".into())))
    }

    async fn check(r: reqwest::Response, what: &str) -> Result<String> {
        let status = r.status();
        let body = r.text().await.unwrap_or_default();
        if !status.is_success() {
            let mut excerpt = body;
            excerpt.truncate(400);
            return Err(Error::Transfer(format!("s3 {what}: {status} {excerpt}")));
        }
        Ok(body)
    }

    pub(crate) async fn create_multipart(&self, object: &str) -> Result<String> {
        let uri = self.uri_for(object);
        let q = vec![("uploads".to_string(), String::new())];
        let r = self
            .request(reqwest::Method::POST, &uri, &q, Vec::new(), &payload_hash(b""), &[])
            .await?;
        let body = Self::check(r, "create multipart").await?;
        xml_tag(&body, "UploadId")
            .ok_or_else(|| Error::Transfer(format!("s3 create multipart: no UploadId in {body:.200}")))
    }

    /// Upload one part; returns its ETag for the completion manifest.
    pub(crate) async fn upload_part(
        &self,
        object: &str,
        upload_id: &str,
        part_number: u32,
        bytes: Vec<u8>,
    ) -> Result<String> {
        let uri = self.uri_for(object);
        let q = vec![
            ("partNumber".to_string(), part_number.to_string()),
            ("uploadId".to_string(), upload_id.to_string()),
        ];
        let r = self
            .request(reqwest::Method::PUT, &uri, &q, bytes, UNSIGNED_PAYLOAD, &[])
            .await?;
        let etag = r
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());
        Self::check(r, "upload part").await?;
        etag.ok_or_else(|| Error::Transfer("s3 upload part: response had no ETag".into()))
    }

    pub(crate) async fn complete_multipart(
        &self,
        object: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<()> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>\"{etag}\"</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let body = xml.into_bytes();
        let hash = payload_hash(&body);
        let uri = self.uri_for(object);
        let q = vec![("uploadId".to_string(), upload_id.to_string())];
        let r = self
            .request(reqwest::Method::POST, &uri, &q, body, &hash, &[])
            .await?;
        let text = Self::check(r, "complete multipart").await?;
        // S3 can return 200 with an <Error> body for late failures.
        if text.contains("<Error>") {
            return Err(Error::Transfer(format!("s3 complete multipart: {text:.400}")));
        }
        Ok(())
    }

    pub(crate) async fn abort_multipart(&self, object: &str, upload_id: &str) {
        let uri = self.uri_for(object);
        let q = vec![("uploadId".to_string(), upload_id.to_string())];
        let _ = self
            .request(reqwest::Method::DELETE, &uri, &q, Vec::new(), &payload_hash(b""), &[])
            .await;
    }

    pub(crate) async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let uri = if self.path_style {
            format!("/{}", enc_seg(&self.bucket))
        } else {
            "/".to_string()
        };
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".to_string(), t.clone()));
            }
            let r = self
                .request(reqwest::Method::GET, &uri, &q, Vec::new(), &payload_hash(b""), &[])
                .await?;
            let body = Self::check(r, "list").await?;
            out.extend(xml_tags(&body, "Key"));
            token = xml_tag(&body, "NextContinuationToken");
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    pub(crate) async fn delete(&self, object: &str) -> Result<()> {
        let uri = self.uri_for(object);
        let r = self
            .request(reqwest::Method::DELETE, &uri, &[], Vec::new(), &payload_hash(b""), &[])
            .await?;
        // 204 for present AND absent keys; other failures still surface.
        Self::check(r, "delete").await.map(|_| ())
    }

    /// Server-side copy (CopyObject, ≤ 5 GiB per object — a per-pipe staging
    /// part stays well under that at any pipe count we ship).
    pub(crate) async fn copy(&self, src: &str, dst: &str) -> Result<()> {
        let uri = self.uri_for(dst);
        let source = format!(
            "/{}/{}",
            self.bucket,
            src.split('/').map(enc_seg).collect::<Vec<_>>().join("/")
        );
        let r = self
            .request(
                reqwest::Method::PUT,
                &uri,
                &[],
                Vec::new(),
                &payload_hash(b""),
                &[("x-amz-copy-source", source)],
            )
            .await?;
        let body = Self::check(r, "copy").await?;
        if body.contains("<Error>") {
            return Err(Error::Transfer(format!("s3 copy: {body:.400}")));
        }
        Ok(())
    }
}

pub(crate) struct S3Sink {
    conn: S3Conn,
    bare: String,
    staging: String,
    names: Arc<Vec<String>>,
    delivered: Arc<Vec<Delivered>>,
    next_part: Arc<AtomicU64>,
}

impl S3Sink {
    pub(crate) fn bind(conn: S3Conn, dest_table: &str, _parallel: usize) -> Result<Self> {
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
}

impl crate::sink::Sink for S3Sink {
    type Loader = S3Loader;

    fn accepts(&self) -> &[WireFormat] {
        &[WireFormat::PgCopyBinary]
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        _mode: Mode,
    ) -> Result<()> {
        let mut delivered = Vec::new();
        let mut names = Vec::new();
        for (c, lc) in plan.cols.iter().zip(lane.cols.iter()) {
            if !parquet_col_ok(&c.udt, c.precision) {
                return Err(Error::InvalidInput(format!(
                    "column {} has type {} — the parquet lane can't encode it; \
                     cast it in a source view (e.g. {}::text)",
                    c.name, c.udt, c.name
                )));
            }
            names.push(c.name.clone());
            delivered.push(lc.delivered.clone());
        }
        self.names = Arc::new(names);
        self.delivered = Arc::new(delivered);
        for obj in self.conn.list(&self.staging).await? {
            self.conn.delete(&obj).await?;
        }
        Ok(())
    }

    async fn loader(&self) -> Result<S3Loader> {
        let n = self.next_part.fetch_add(1, Ordering::Relaxed);
        let object = format!("{}part-{n:05}.parquet", self.staging);
        let upload_id = self.conn.create_multipart(&object).await?;
        Ok(S3Loader {
            conn: self.conn.clone(),
            object,
            upload_id,
            etags: Vec::new(),
            part_no: 0,
            pq: ParquetEncoder::new(self.names.as_ref().clone(), self.delivered.as_ref().clone(), None)?,
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
            for p in &parts {
                self.conn.delete(p).await?;
            }
            return Ok(());
        }
        let dir = format!("{}{}/", self.conn.prefix, self.bare);
        let stale: Vec<String> = self.conn.list(&dir).await?;
        let mut fresh = Vec::with_capacity(parts.len());
        for (i, p) in parts.iter().enumerate() {
            let dst = format!("{dir}part-{i:05}.parquet");
            self.conn.copy(p, &dst).await?;
            fresh.push(dst);
        }
        // Same non-transactional caveat as the GCS sink: readers can see a
        // mixed directory until the sweep finishes; only OUR part files are
        // ever deleted.
        for s in stale {
            if !fresh.contains(&s) && is_part_object(&s) {
                let _ = self.conn.delete(&s).await;
            }
        }
        for p in &parts {
            let _ = self.conn.delete(p).await;
        }
        Ok(())
    }
}

/// apitap's own part-file names — the ONLY things a stale-sweep may delete.
fn is_part_object(name: &str) -> bool {
    let base = name.rsplit_once('/').map_or(name, |(_, b)| b);
    base.strip_prefix("part-")
        .and_then(|r| r.strip_suffix(".parquet"))
        .is_some_and(|digits| digits.len() == 5 && digits.bytes().all(|b| b.is_ascii_digit()))
}

pub(crate) struct S3Loader {
    conn: S3Conn,
    object: String,
    upload_id: String,
    etags: Vec<(u32, String)>,
    part_no: u32,
    pq: ParquetEncoder,
    rows: u64,
}

impl S3Loader {
    /// Upload everything buffered as the next part. Unlike GCS chunks, S3
    /// parts need no alignment — only the ≥5 MiB floor for non-final parts,
    /// guaranteed by the send threshold; `final_part` lifts the floor.
    async fn flush_part(&mut self, bytes: Vec<u8>) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.part_no += 1;
        let etag = self
            .conn
            .upload_part(&self.object, &self.upload_id, self.part_no, bytes)
            .await?;
        self.etags.push((self.part_no, etag));
        Ok(())
    }
}

impl crate::sink::Loader for S3Loader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        self.rows += self.pq.push(&buf)?;
        let pending = {
            let mut b = self.pq.out.0.lock().expect("parquet buf");
            if b.len() < SEND_THRESHOLD {
                return Ok(());
            }
            std::mem::take(&mut *b)
        };
        self.flush_part(pending).await
    }

    async fn finish(mut self) -> Result<u64> {
        self.pq.finish_file()?;
        let pending = {
            let mut b = self.pq.out.0.lock().expect("parquet buf");
            std::mem::take(&mut *b)
        };
        if self.etags.is_empty() && pending.is_empty() {
            // Nothing was ever produced (0-row worker): abort the upload so no
            // empty object lands; finalize's 0-row guard handles the rest.
            self.conn.abort_multipart(&self.object, &self.upload_id).await;
            return Ok(0);
        }
        self.flush_part(pending).await?;
        self.conn
            .complete_multipart(&self.object, &self.upload_id, &self.etags)
            .await?;
        Ok(self.rows)
    }

    async fn abort(self, cause: Error) -> Error {
        self.conn.abort_multipart(&self.object, &self.upload_id).await;
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_extraction_handles_s3_shapes() {
        let body = "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
                    <UploadId>abc+/=xyz</UploadId></InitiateMultipartUploadResult>";
        assert_eq!(xml_tag(body, "UploadId").as_deref(), Some("abc+/=xyz"));
        let listing = "<ListBucketResult><Contents><Key>a/part-00000.parquet</Key></Contents>\
                       <Contents><Key>a/part-00001.parquet</Key></Contents></ListBucketResult>";
        assert_eq!(
            xml_tags(listing, "Key"),
            vec!["a/part-00000.parquet", "a/part-00001.parquet"]
        );
        assert!(xml_tag(listing, "NextContinuationToken").is_none());
    }

    #[test]
    fn stale_sweep_only_touches_our_part_files() {
        assert!(is_part_object("lake/events/part-00003.parquet"));
        assert!(!is_part_object("lake/events/part-3.parquet"));
        assert!(!is_part_object("lake/events/users-notes.parquet"));
        assert!(!is_part_object("lake/events/README.md"));
    }

    #[test]
    fn enc_seg_and_decode_component_roundtrip() {
        assert_eq!(enc_seg("año 1.parquet"), "a%C3%B1o%201.parquet");
        assert_eq!(decode_component("a%C3%B1o%201").unwrap(), "año 1");
        assert_eq!(decode_component("k+ey").unwrap(), "k+ey");
        assert!(decode_component("bad%zz").is_err());
    }
}
