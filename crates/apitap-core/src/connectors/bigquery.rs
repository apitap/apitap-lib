//! Google BigQuery destination. REST only — no gRPC — so it rides the same
//! reqwest stack as everything else.
//!
//! Ingest path (chosen for wall-clock, measured priorities: upload bandwidth is
//! the wall from a datacenter box, load-job execution parallelizes across jobs):
//!   - the source's TabSeparated lane is transcoded in-flight to gzipped NDJSON
//!     (explicit schema — nothing is inferred; NULL fields are omitted, so sparse
//!     rows shrink; gzip flattens NDJSON's key overhead),
//!   - each worker streams into ONE resumable-upload load job (constant memory,
//!     N workers = N parallel load jobs = far under BigQuery's 1,500 loads per
//!     table per day even at hourly schedules),
//!   - jobs land in a staging table; finalize moves the whole thing with a COPY
//!     job — atomic, metadata-only, free — `WRITE_TRUNCATE` for replace,
//!     `WRITE_APPEND` for append.
//!
//! Streaming `insertAll` is avoided on purpose: it bills per byte, and rows sit
//! in the streaming buffer where DML/copies can't see them. Load and copy jobs
//! are free.
//!
//! Auth: a service-account key (JSON key file) is exchanged for a ~1h OAuth2
//! access token via the JWT-bearer grant (RS256). The private key never leaves
//! the process. Incremental state lives in `<dataset>._apitap_state`, upserted
//! with a single-statement MERGE right after the copy job commits; the watermark
//! is `greatest(state, data)`, so a crash between the two costs a bounded
//! re-read, never a skip.

use crate::driver::Loader;
use crate::error::{Error, Result};
use crate::plan::{Delivered, DestState, Lane, TablePlan, WireFormat};
use crate::Mode;
use serde_json::{json, Value};
use std::sync::Arc;

const BQ_BASE: &str = "https://bigquery.googleapis.com/bigquery/v2";
const BQ_UPLOAD: &str = "https://bigquery.googleapis.com/upload/bigquery/v2";
const BQ_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
/// Resumable-upload chunks must be 256 KiB multiples (Google's contract);
/// 8 MiB per PUT amortizes round-trips without holding much gzip output.
const UPLOAD_ALIGN: usize = 256 * 1024;
const UPLOAD_CHUNK: usize = 8 * 1024 * 1024;
/// Rotate to a NEW load job once a file reaches this many COMPRESSED bytes —
/// but only if ROTATE_SECS have also passed: BigQuery allows ~5 metadata
/// updates per 10s PER TABLE, and a fast worker sealing a 12 MiB file every
/// second tripped rateLimitExceeded at 10M rows. The byte floor keeps files
/// worth parsing; the time gate caps the per-staging job rate at ~2/10s; the
/// hard cap bounds the last file's single-threaded parse tail (gzip isn't
/// splittable — modest files parallelize server-side and their jobs poll in
/// the background while the next file streams).
const ROTATE_BYTES: u64 = 12 * 1024 * 1024;
const ROTATE_SECS: u64 = 6;
const ROTATE_HARD_BYTES: u64 = 96 * 1024 * 1024;
const STATE_TABLE: &str = "_apitap_state";

// ============================================================================
// Service-account auth (JWT-bearer → OAuth2 access token)
// ============================================================================

#[derive(serde::Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

#[derive(serde::Serialize)]
struct JwtClaims {
    iss: String,
    scope: &'static str,
    aud: String,
    iat: u64,
    exp: u64,
}

async fn fetch_access_token(client: &reqwest::Client, credentials_json: &str) -> Result<String> {
    let key: ServiceAccountKey = serde_json::from_str(credentials_json)
        .map_err(|e| Error::InvalidInput(format!("invalid BigQuery service-account JSON: {e}")))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Transfer(format!("system clock: {e}")))?
        .as_secs();
    let claims = JwtClaims {
        iss: key.client_email,
        scope: BQ_SCOPE,
        aud: key.token_uri.clone(),
        iat: now,
        exp: now + 3600,
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(key.private_key.as_bytes())
        .map_err(|e| {
            Error::InvalidInput(format!("invalid BigQuery service-account private_key: {e}"))
        })?;
    let assertion = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &encoding_key,
    )
    .map_err(|e| Error::Transfer(format!("failed to sign BigQuery JWT: {e}")))?;
    let resp = client
        .post(&key.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion.as_str()),
        ])
        .send()
        .await
        .map_err(|e| Error::Transfer(format!("bigquery token exchange: {e}")))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(Error::Transfer(format!(
            "bigquery token exchange failed ({status}): {}",
            body.trim()
        )));
    }
    let v: Value = serde_json::from_str(&body)
        .map_err(|e| Error::Transfer(format!("bigquery token response: {e}")))?;
    v["access_token"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| Error::Transfer("bigquery token response missing access_token".into()))
}

// ============================================================================
// Connection: project/dataset + bearer-authenticated REST helpers
// ============================================================================

#[derive(Clone)]
pub(crate) struct BqConn {
    client: reqwest::Client,
    /// Service-account key JSON, kept to re-mint the ~1h token mid-transfer.
    credentials: Arc<String>,
    /// (bearer token, minted-at) — refreshed behind the lock at 55 minutes.
    auth: Arc<tokio::sync::Mutex<(String, std::time::Instant)>>,
    pub project: String,
    pub dataset: String,
    location: Option<String>,
}

impl BqConn {
    /// `bigquery://<project>/<dataset>?credentials=/path/key.json[&location=EU]`;
    /// without `credentials` the `GOOGLE_APPLICATION_CREDENTIALS` env var is used.
    pub(crate) async fn parse(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidInput(format!("bigquery url: {e}")))?;
        let project = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "bigquery url needs a project: bigquery://<project>/<dataset>".into(),
                )
            })?
            .to_string();
        let mut segments = u.path().trim_matches('/').split('/');
        let dataset = segments
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "bigquery url needs a dataset: bigquery://<project>/<dataset>".into(),
                )
            })?
            .to_string();
        if segments.next().is_some() {
            return Err(Error::InvalidInput(
                "bigquery url has extra path segments after the dataset — use \
                 bigquery://<project>/<dataset> and pass the table via dest_table"
                    .into(),
            ));
        }
        let mut credentials_path = None;
        let mut location = None;
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "credentials" => credentials_path = Some(v.to_string()),
                "location" => location = Some(v.to_string()),
                other => {
                    return Err(Error::InvalidInput(format!(
                    "unknown bigquery url parameter '{other}' (supported: credentials, location)"
                )))
                }
            }
        }
        let credentials_path = credentials_path
            .or_else(|| std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "bigquery needs a service-account key: add ?credentials=/path/key.json \
                     to the url or set GOOGLE_APPLICATION_CREDENTIALS"
                        .into(),
                )
            })?;
        let credentials = std::fs::read_to_string(&credentials_path).map_err(|e| {
            Error::InvalidInput(format!(
                "can't read bigquery credentials {credentials_path}: {e}"
            ))
        })?;
        let client = reqwest::Client::new();
        let token = fetch_access_token(&client, &credentials).await?;
        Ok(Self {
            client,
            credentials: Arc::new(credentials),
            auth: Arc::new(tokio::sync::Mutex::new((token, std::time::Instant::now()))),
            project,
            dataset,
            location,
        })
    }

    /// The current bearer token, re-minted past 55 minutes so a long transfer
    /// never hits a mid-flight 401.
    async fn bearer(&self) -> Result<String> {
        let mut auth = self.auth.lock().await;
        if auth.1.elapsed() >= std::time::Duration::from_secs(55 * 60) {
            auth.0 = fetch_access_token(&self.client, &self.credentials).await?;
            auth.1 = std::time::Instant::now();
        }
        Ok(auth.0.clone())
    }

    async fn api(
        &self,
        method: reqwest::Method,
        url: String,
        body: Option<&Value>,
    ) -> Result<Value> {
        let mut req = self
            .client
            .request(method.clone(), &url)
            .bearer_auth(self.bearer().await?);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("bigquery {method} {url}: {e}")))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Transfer(format!(
                "bigquery {status} on {url}: {}",
                text.trim()
            )));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|e| Error::Transfer(format!("bigquery response parse: {e}")))
    }

    /// Like [`api`], but a 404 becomes `Ok(None)` (probe / idempotent-delete calls).
    async fn api_opt(
        &self,
        method: reqwest::Method,
        url: String,
        body: Option<&Value>,
    ) -> Result<Option<Value>> {
        match self.api(method, url, body).await {
            Ok(v) => Ok(Some(v)),
            Err(Error::Transfer(m)) if m.contains("404 Not Found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn table_url(&self, table: &str) -> String {
        format!(
            "{BQ_BASE}/projects/{}/datasets/{}/tables/{table}",
            self.project, self.dataset
        )
    }

    async fn table_get(&self, table: &str) -> Result<Option<Value>> {
        self.api_opt(reqwest::Method::GET, self.table_url(table), None)
            .await
    }

    async fn table_delete(&self, table: &str) -> Result<()> {
        self.api_opt(reqwest::Method::DELETE, self.table_url(table), None)
            .await
            .map(|_| ())
    }

    async fn table_create(&self, table: &str, fields: &Value) -> Result<()> {
        let body = json!({
            "tableReference": {
                "projectId": self.project, "datasetId": self.dataset, "tableId": table
            },
            "schema": { "fields": fields },
        });
        self.api(
            reqwest::Method::POST,
            format!(
                "{BQ_BASE}/projects/{}/datasets/{}/tables",
                self.project, self.dataset
            ),
            Some(&body),
        )
        .await
        .map(|_| ())
    }

    /// Table ids in the dataset starting with `prefix` (one page is plenty —
    /// worker counts are small).
    async fn tables_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let v = self
            .api(
                reqwest::Method::GET,
                format!(
                    "{BQ_BASE}/projects/{}/datasets/{}/tables?maxResults=1000",
                    self.project, self.dataset
                ),
                None,
            )
            .await?;
        Ok(v["tables"]
            .as_array()
            .map(|ts| {
                ts.iter()
                    .filter_map(|t| t["tableReference"]["tableId"].as_str())
                    .filter(|id| id.starts_with(prefix))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn ensure_dataset(&self) -> Result<()> {
        let url = format!(
            "{BQ_BASE}/projects/{}/datasets/{}",
            self.project, self.dataset
        );
        match self.api_opt(reqwest::Method::GET, url, None).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => {
                let mut body = json!({
                    "datasetReference": {"projectId": self.project, "datasetId": self.dataset},
                });
                if let Some(loc) = &self.location {
                    body["location"] = json!(loc);
                }
                self.api(
                    reqwest::Method::POST,
                    format!("{BQ_BASE}/projects/{}/datasets", self.project),
                    Some(&body),
                )
                .await
                .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    /// Run a standard-SQL query and return rows as text cells (None = NULL).
    async fn query(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let mut body = json!({
            "query": sql, "useLegacySql": false, "timeoutMs": 60_000,
        });
        if let Some(loc) = &self.location {
            body["location"] = json!(loc);
        }
        let v = self
            .api(
                reqwest::Method::POST,
                format!("{BQ_BASE}/projects/{}/queries", self.project),
                Some(&body),
            )
            .await?;
        if v["jobComplete"].as_bool() != Some(true) {
            // One long-poll retry via getQueryResults; queries here are tiny.
            let job_id = v["jobReference"]["jobId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let loc = v["jobReference"]["location"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let v2 = self
                .api(
                    reqwest::Method::GET,
                    format!(
                        "{BQ_BASE}/projects/{}/queries/{job_id}?timeoutMs=60000&location={loc}",
                        self.project
                    ),
                    None,
                )
                .await?;
            if v2["jobComplete"].as_bool() != Some(true) {
                return Err(Error::Transfer(format!(
                    "bigquery query did not complete in time: {sql}"
                )));
            }
            return Ok(rows_of(&v2));
        }
        Ok(rows_of(&v))
    }

    /// Load a handful of NDJSON rows into `table` with one multipart load job.
    /// Free-tier safe: the state machinery must never need DML (sandbox projects
    /// reject it), so state rows are APPENDED and readers take the newest.
    async fn load_rows(&self, table: &str, ndjson: Vec<u8>) -> Result<()> {
        let config = json!({
            "configuration": { "load": {
                "destinationTable": {
                    "projectId": self.project, "datasetId": self.dataset,
                    "tableId": table,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "maxBadRecords": 0,
            }}
        });
        let boundary = "apitap_state_boundary";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Type: application/json; charset=UTF-8\r\n\r\n");
        body.extend_from_slice(config.to_string().as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&ndjson);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let url = format!(
            "{BQ_UPLOAD}/projects/{}/jobs?uploadType=multipart",
            self.project
        );
        let resp = self
            .client
            .post(&url)
            .bearer_auth(self.bearer().await?)
            .header(
                "Content-Type",
                format!("multipart/related; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("bigquery state load: {e}")))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Transfer(format!(
                "bigquery state load {status}: {}",
                text.trim()
            )));
        }
        let v: Value = serde_json::from_str(&text)
            .map_err(|e| Error::Transfer(format!("bigquery state load response: {e}")))?;
        let job_id = v["jobReference"]["jobId"]
            .as_str()
            .ok_or_else(|| Error::Transfer("bigquery state load missing jobId".into()))?
            .to_string();
        let loc = v["jobReference"]["location"].as_str().map(str::to_string);
        self.poll_job(&job_id, loc.as_deref()).await.map(|_| ())
    }

    /// Poll a load/copy job to DONE; adaptive backoff (dense early — small jobs
    /// finish in seconds; cheap 2s cadence after).
    async fn poll_job(&self, job_id: &str, location: Option<&str>) -> Result<Value> {
        let loc = location
            .map(|l| format!("?location={l}"))
            .unwrap_or_default();
        let url = format!("{BQ_BASE}/projects/{}/jobs/{job_id}{loc}", self.project);
        for i in 0..900u32 {
            let v = self.api(reqwest::Method::GET, url.clone(), None).await?;
            if v["status"]["state"].as_str() == Some("DONE") {
                if let Some(err) = v["status"]["errorResult"].as_object() {
                    return Err(Error::Transfer(format!(
                        "bigquery job {job_id} failed: {}",
                        serde_json::to_string(err).unwrap_or_default()
                    )));
                }
                return Ok(v);
            }
            let ms = if i < 24 { 500 } else { 2_000 };
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
        Err(Error::Transfer(format!(
            "bigquery job {job_id} still running after 30 minutes"
        )))
    }
}

fn rows_of(v: &Value) -> Vec<Vec<Option<String>>> {
    v["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    r["f"]
                        .as_array()
                        .map(|cells| {
                            cells
                                .iter()
                                .map(|c| c["v"].as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn sql_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

// ============================================================================
// Types: what the TSV lane delivers → BigQuery standard SQL types
// ============================================================================

fn bq_type_of(d: &Delivered) -> &'static str {
    match d {
        // u64 can exceed INT64; NUMERIC(20,0) holds it exactly.
        Delivered::Int {
            bytes: 8,
            unsigned: true,
        } => "NUMERIC",
        Delivered::Int { .. } => "INT64",
        Delivered::Float32 | Delivered::Float64 => "FLOAT64",
        // NUMERIC: ≤29 integer digits, scale ≤9; BIGNUMERIC: ≤38.38.
        Delivered::Decimal { p: 0, .. } => "BIGNUMERIC",
        Delivered::Decimal { p, s } if p.saturating_sub(*s) <= 29 && *s <= 9 => "NUMERIC",
        Delivered::Decimal { p, s } if p.saturating_sub(*s) <= 38 && *s <= 38 => "BIGNUMERIC",
        Delivered::Decimal { .. } => "STRING",
        Delivered::Bool => "BOOL",
        Delivered::Date => "DATE",
        Delivered::DateTime { utc: true } => "TIMESTAMP",
        Delivered::DateTime { utc: false } => "DATETIME",
        Delivered::Uuid => "STRING",
        Delivered::Json => "JSON",
        Delivered::Text => "STRING",
        Delivered::Bytes => "BYTES",
    }
}

/// BigQuery column names are `[A-Za-z_][A-Za-z0-9_]*` up to 300 chars. Rather
/// than silently renaming (and diverging from the source), reject loudly.
fn check_col_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.len() <= 300;
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "column name '{name}' is not a valid BigQuery column name \
             ([A-Za-z_][A-Za-z0-9_]*) — alias it in a source view"
        )))
    }
}

// ============================================================================
// TSV → NDJSON transcode (buffers arrive record-aligned per the Loader contract)
// ============================================================================

/// Un-escape one PostgreSQL text-COPY field into `out`. Only called when a
/// backslash was seen — the common case borrows the raw bytes.
fn unescape_into(field: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < field.len() {
        // Bulk-copy up to the next backslash (rare in real data).
        let mut j = i;
        while j < field.len() && field[j] != b'\\' {
            j += 1;
        }
        out.extend_from_slice(&field[i..j]);
        if j >= field.len() {
            break;
        }
        if j + 1 < field.len() {
            out.push(match field[j + 1] {
                b'b' => 0x08,
                b'f' => 0x0c,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'v' => 0x0b,
                other => other, // covers \\ and any literal escape
            });
            i = j + 2;
        } else {
            out.push(b'\\');
            i = j + 1;
        }
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_into(data: &[u8], out: &mut Vec<u8>) {
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        out.push(B64[(b[0] >> 2) as usize]);
        out.push(B64[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize]);
        out.push(if chunk.len() > 1 {
            B64[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize]
        } else {
            b'='
        });
        out.push(if chunk.len() > 2 {
            B64[(b[2] & 0x3f) as usize]
        } else {
            b'='
        });
    }
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

/// Bytes that force a CSV field into quotes.
static CSV_NEEDS_QUOTE: [bool; 256] = {
    let mut t = [false; 256];
    t[b',' as usize] = true;
    t[b'"' as usize] = true;
    t[b'\n' as usize] = true;
    t[b'\r' as usize] = true;
    t
};

/// Quote a CSV field: wrap in quotes, double any embedded quote. Bulk-copies
/// clean runs; embedded newlines are legal (the load sets allowQuotedNewlines).
fn csv_quote_into(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    let mut i = 0;
    while i < s.len() {
        let mut j = i;
        while j < s.len() && s[j] != b'"' {
            j += 1;
        }
        out.extend_from_slice(&s[i..j]);
        if j == s.len() {
            break;
        }
        out.extend_from_slice(b"\"\"");
        i = j + 1;
    }
    out.push(b'"');
}

/// Append one field's CSV form for `d`. `raw` is the still-escaped TSV field.
/// BigQuery CSV semantics (probed live): unquoted empty = NULL, quoted "" =
/// empty string — so a present value that is empty or contains a delimiter,
/// quote, or newline gets quoted; everything else rides raw.
fn append_csv_value(raw: &[u8], d: &Delivered, out: &mut Vec<u8>, scratch: &mut Vec<u8>) {
    scratch.clear();
    let val: &[u8] = if raw.contains(&b'\\') {
        unescape_into(raw, scratch);
        scratch.as_slice()
    } else {
        raw
    };
    match d {
        // Numbers, dates, uuids: never contain CSV specials — raw. NaN and
        // Infinity are accepted FLOAT64 tokens; a NUMERIC NaN fails the load
        // loudly, same as every other destination.
        Delivered::Int { .. }
        | Delivered::Decimal { .. }
        | Delivered::Float32
        | Delivered::Float64
        | Delivered::Date
        | Delivered::DateTime { utc: false }
        | Delivered::Uuid => out.extend_from_slice(val),
        Delivered::Bool => out.extend_from_slice(if val == b"t" { b"true" } else { b"false" }),
        Delivered::DateTime { utc: true } => {
            // Postgres (UTC session) renders "…+00"; BigQuery wants a full offset.
            out.extend_from_slice(val);
            if val.len() > 3 && matches!(val[val.len() - 3], b'+' | b'-') {
                out.extend_from_slice(b":00");
            }
        }
        Delivered::Bytes => {
            // bytea_output=hex: "\x<hex>" — base64 has no CSV specials.
            let hex = val.strip_prefix(b"\\x").unwrap_or(val);
            let bytes: Vec<u8> = hex
                .chunks(2)
                .map(|p| (hex_val(p[0]) << 4) | p.get(1).map(|&b| hex_val(b)).unwrap_or(0))
                .collect();
            base64_into(&bytes, out);
        }
        Delivered::Json | Delivered::Text => {
            if val.is_empty() || val.iter().any(|&b| CSV_NEEDS_QUOTE[b as usize]) {
                csv_quote_into(val, out);
            } else {
                out.extend_from_slice(val);
            }
        }
    }
}

/// Re-export for the parquet encoder (bqparquet.rs tracks its own cursor max).
pub(crate) fn wm_max_pub(a: Option<String>, b: Option<String>, numeric: bool) -> Option<String> {
    wm_max(a, b, numeric)
}

/// The larger of two watermark texts — numeric cursors compare numerically,
/// text cursors (timestamps in the source's own rendering) lexicographically.
fn wm_max(a: Option<String>, b: Option<String>, numeric: bool) -> Option<String> {
    match (a, b) {
        (Some(x), Some(y)) => {
            let x_wins = if numeric {
                match (x.parse::<i128>(), y.parse::<i128>()) {
                    (Ok(xi), Ok(yi)) => xi >= yi,
                    (Ok(_), Err(_)) => true,
                    (Err(_), Ok(_)) => false,
                    _ => x >= y,
                }
            } else {
                x >= y
            };
            Some(if x_wins { x } else { y })
        }
        (a, None) => a,
        (None, b) => b,
    }
}

/// Transcode record-aligned TSV bytes into CSV. Returns rows converted.
/// NULL fields become unquoted-empty (= NULL to BigQuery; probed live), and
/// `cursor` = (column index, numeric) tracks the running MAX of that column
/// into `wm` — the staged watermark comes from here for free instead of a
/// billed MAX() query against the staging table.
fn tsv_to_csv(
    input: &[u8],
    delivered: &[Delivered],
    cursor: Option<(usize, bool)>,
    wm: &mut Option<String>,
    out: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> Result<u64> {
    let mut rows = 0u64;
    // split_inclusive: no trailing artifact to skip, so a genuinely empty line
    // (a single empty-string text column) stays a REAL record.
    for piece in input.split_inclusive(|&b| b == b'\n') {
        let line = piece.strip_suffix(b"\n").unwrap_or(piece);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let mut col = 0usize;
        for field in line.split(|&b| b == b'\t') {
            if col >= delivered.len() {
                return Err(Error::Transfer(format!(
                    "tsv row has more fields than the {} planned columns",
                    delivered.len()
                )));
            }
            if col > 0 {
                out.push(b',');
            }
            if field != b"\\N" {
                append_csv_value(field, &delivered[col], out, scratch);
                if let Some((idx, numeric)) = cursor {
                    if col == idx {
                        // Cursor values (ints, timestamps) never carry escapes.
                        let v = String::from_utf8_lossy(field).into_owned();
                        *wm = wm_max(wm.take(), Some(v), numeric);
                    }
                }
            }
            col += 1;
        }
        if col != delivered.len() {
            return Err(Error::Transfer(format!(
                "tsv row has {col} fields, expected {}",
                delivered.len()
            )));
        }
        out.push(b'\n');
        rows += 1;
    }
    Ok(rows)
}

// ============================================================================
// Loader: one resumable-upload load job per worker
// ============================================================================

pub(crate) struct BqLoader {
    conn: BqConn,
    job_config: Value,
    /// This worker's OWN staging table: BigQuery rate-limits table update
    /// operations PER TABLE (~1.5/s) — many workers' load jobs against one
    /// shared staging tripped rateLimitExceeded at 10M rows. finalize copies
    /// every worker staging into the final table in ONE copy job.
    staging_table: String,
    registry: Arc<std::sync::Mutex<Vec<String>>>,
    registered: bool,
    delivered: Arc<Vec<Delivered>>,
    cursor: Option<(usize, bool)>,
    local_wm: Option<String>,
    shared_wm: Arc<std::sync::Mutex<Option<String>>>,
    /// Parquet lane encoder; None = the CSV/gzip lane.
    pq: Option<crate::bqparquet::ParquetEncoder>,
    /// Rows in the CURRENT file (cross-checked against its job's outputRows).
    file_rows: u64,
    /// Completed files' jobs, polling in the background while later files
    /// stream — each resolves to its committed row count.
    pending_jobs: Vec<tokio::task::JoinHandle<Result<u64>>>,
    last_seal: std::time::Instant,
    /// APITAP_DEBUG=1: cumulative per-phase wall time, reported at finish.
    t_transcode: std::time::Duration,
    t_gzip: std::time::Duration,
    t_upload: std::time::Duration,
    gz: flate2::write::GzEncoder<Vec<u8>>,
    session_uri: Option<String>,
    offset: u64,
    rows: u64,
    ndjson: Vec<u8>,
    scratch: Vec<u8>,
}

impl BqLoader {
    #[allow(clippy::too_many_arguments)]
    fn open(
        conn: BqConn,
        base_config: &Value,
        staging_table: String,
        registry: Arc<std::sync::Mutex<Vec<String>>>,
        names: &[String],
        delivered: Arc<Vec<Delivered>>,
        parquet_lane: bool,
        cursor: Option<(usize, bool)>,
        shared_wm: Arc<std::sync::Mutex<Option<String>>>,
    ) -> Result<Self> {
        let mut job_config = base_config.clone();
        job_config["configuration"]["load"]["destinationTable"]["tableId"] = json!(staging_table);
        let pq = if parquet_lane {
            Some(crate::bqparquet::ParquetEncoder::new(
                names.to_vec(),
                delivered.as_ref().clone(),
                cursor,
            )?)
        } else {
            None
        };
        Ok(Self {
            conn,
            job_config,
            staging_table,
            registry,
            registered: false,
            pq,
            delivered,
            cursor,
            local_wm: None,
            shared_wm,
            file_rows: 0,
            pending_jobs: Vec::new(),
            last_seal: std::time::Instant::now(),
            t_transcode: Default::default(),
            t_gzip: Default::default(),
            t_upload: Default::default(),
            gz: flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast()),
            session_uri: None,
            offset: 0,
            rows: 0,
            ndjson: Vec::new(),
            scratch: Vec::new(),
        })
    }

    /// Bytes waiting in the current file's encoder output.
    fn pending_len(&self) -> usize {
        match &self.pq {
            Some(pq) => pq.out.0.lock().expect("parquet buf").len(),
            None => self.gz.get_ref().len(),
        }
    }

    /// Take up to `take` buffered output bytes (caller picks an aligned count).
    fn take_pending(&mut self, take: usize) -> Vec<u8> {
        match &mut self.pq {
            Some(pq) => pq
                .out
                .0
                .lock()
                .expect("parquet buf")
                .drain(..take)
                .collect(),
            None => self.gz.get_mut().drain(..take).collect(),
        }
    }

    async fn begin_session(&mut self) -> Result<()> {
        let url = format!(
            "{BQ_UPLOAD}/projects/{}/jobs?uploadType=resumable",
            self.conn.project
        );
        let resp = self
            .conn
            .client
            .post(&url)
            .bearer_auth(self.conn.bearer().await?)
            .header("X-Upload-Content-Type", "application/octet-stream")
            .json(&self.job_config)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("bigquery resumable begin: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Transfer(format!(
                "bigquery resumable begin {status}: {}",
                body.trim()
            )));
        }
        let uri = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| {
                Error::Transfer("bigquery resumable begin: no session Location header".into())
            })?;
        self.session_uri = Some(uri);
        Ok(())
    }

    /// PUT `chunk` at the current offset. `total` = Some(len) marks the final chunk.
    async fn put_chunk(&mut self, chunk: Vec<u8>, total: Option<u64>) -> Result<Option<Value>> {
        if self.session_uri.is_none() {
            self.begin_session().await?;
        }
        let uri = self.session_uri.clone().expect("session begun above");
        let end = self.offset + chunk.len() as u64;
        let range = match total {
            Some(t) if chunk.is_empty() => format!("bytes */{t}"),
            Some(t) => format!("bytes {}-{}/{t}", self.offset, end - 1),
            None => format!("bytes {}-{}/*", self.offset, end - 1),
        };
        // Re-PUTting a byte range is idempotent in the resumable protocol, so
        // transient network errors and 5xx get a couple of paced retries.
        let mut resp = None;
        for attempt in 0..3u32 {
            let r = self
                .conn
                .client
                .put(&uri)
                .bearer_auth(self.conn.bearer().await?)
                .header("Content-Range", range.clone())
                .header("Content-Length", chunk.len().to_string())
                .body(chunk.clone())
                .send()
                .await;
            match r {
                Ok(r) if r.status().is_server_error() && attempt < 2 => {}
                Ok(r) => {
                    resp = Some(r);
                    break;
                }
                Err(e) if attempt < 2 => {
                    let _ = e; // transient; retried below
                }
                Err(e) => return Err(Error::Transfer(format!("bigquery upload chunk: {e}"))),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt as u64 + 1))).await;
        }
        let resp =
            resp.ok_or_else(|| Error::Transfer("bigquery upload chunk: retries exhausted".into()))?;
        let status = resp.status().as_u16();
        match (status, total) {
            // 308 = chunk accepted, upload incomplete.
            (308, None) => {
                self.offset = end;
                Ok(None)
            }
            (200 | 201, Some(_)) => {
                let body = resp.text().await.unwrap_or_default();
                let v: Value = serde_json::from_str(&body)
                    .map_err(|e| Error::Transfer(format!("bigquery job response: {e}")))?;
                Ok(Some(v))
            }
            _ => {
                let body = resp.text().await.unwrap_or_default();
                Err(Error::Transfer(format!(
                    "bigquery upload chunk unexpected status {status}: {}",
                    body.trim()
                )))
            }
        }
    }

    /// Close the CURRENT file: final PUT, then poll its load job in a
    /// background task so the next file streams while BigQuery parses this
    /// one. `expect_rows` is cross-checked against the job's outputRows.
    async fn seal_file(&mut self) -> Result<()> {
        if !self.registered {
            self.registry
                .lock()
                .expect("registry lock")
                .push(self.staging_table.clone());
            self.registered = true;
        }
        let tail: Vec<u8> = match &mut self.pq {
            Some(pq) => {
                // Footer lands in the shared buffer; a fresh writer starts the
                // next file.
                pq.finish_file()?;
                let mut b = pq.out.0.lock().expect("parquet buf");
                b.drain(..).collect()
            }
            None => std::mem::replace(
                &mut self.gz,
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast()),
            )
            .finish()
            .map_err(|e| Error::Transfer(format!("gzip finish: {e}")))?,
        };
        let total = self.offset + tail.len() as u64;
        let job = self
            .put_chunk(tail, Some(total))
            .await?
            .ok_or_else(|| Error::Transfer("bigquery final chunk returned no job".into()))?;
        let job_id = job["jobReference"]["jobId"]
            .as_str()
            .ok_or_else(|| Error::Transfer("bigquery job response missing jobId".into()))?
            .to_string();
        let location = job["jobReference"]["location"].as_str().map(str::to_string);
        let conn = self.conn.clone();
        let expect_rows = self.file_rows;
        self.pending_jobs.push(tokio::spawn(async move {
            let done = conn.poll_job(&job_id, location.as_deref()).await?;
            let out_rows: u64 = done["statistics"]["load"]["outputRows"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if out_rows != expect_rows {
                return Err(Error::Transfer(format!(
                    "bigquery load job {job_id} landed {out_rows} rows, worker \
                     sent {expect_rows} — refusing to continue on a partial load"
                )));
            }
            Ok(out_rows)
        }));
        // Fresh file: new resumable session, offsets from zero.
        self.session_uri = None;
        self.offset = 0;
        self.file_rows = 0;
        Ok(())
    }

    /// Ship every full 256 KiB-aligned span the encoder's buffer holds.
    async fn drain_aligned(&mut self) -> Result<()> {
        loop {
            let ready = self.pending_len();
            if ready < UPLOAD_CHUNK {
                return Ok(());
            }
            let take = (ready / UPLOAD_ALIGN) * UPLOAD_ALIGN;
            let chunk = self.take_pending(take);
            self.put_chunk(chunk, None).await?;
        }
    }
}

impl Loader for BqLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        use std::io::Write;
        let t0 = std::time::Instant::now();
        let t2;
        match &mut self.pq {
            Some(pq) => {
                let added = pq.push(&buf)?;
                self.rows += added;
                self.file_rows += added;
                t2 = std::time::Instant::now();
                self.t_transcode += t2 - t0;
            }
            None => {
                self.ndjson.clear();
                let added = tsv_to_csv(
                    &buf,
                    &self.delivered,
                    self.cursor,
                    &mut self.local_wm,
                    &mut self.ndjson,
                    &mut self.scratch,
                )?;
                self.rows += added;
                self.file_rows += added;
                let t1 = std::time::Instant::now();
                self.gz
                    .write_all(&self.ndjson)
                    .map_err(|e| Error::Transfer(format!("gzip encode: {e}")))?;
                t2 = std::time::Instant::now();
                self.t_transcode += t1 - t0;
                self.t_gzip += t2 - t1;
            }
        }
        self.drain_aligned().await?;
        let file_bytes = self.offset + self.pending_len() as u64;
        if file_bytes >= ROTATE_HARD_BYTES
            || (file_bytes >= ROTATE_BYTES && self.last_seal.elapsed().as_secs() >= ROTATE_SECS)
        {
            self.seal_file().await?;
            self.last_seal = std::time::Instant::now();
        }
        self.t_upload += t2.elapsed();
        Ok(())
    }

    async fn finish(mut self) -> Result<u64> {
        if self.file_rows > 0 || self.session_uri.is_some() {
            self.seal_file().await?;
        }
        let t_poll = std::time::Instant::now();
        let mut committed = 0u64;
        let n_jobs = self.pending_jobs.len();
        for handle in std::mem::take(&mut self.pending_jobs) {
            committed += handle
                .await
                .map_err(|e| Error::Transfer(format!("bigquery job task: {e}")))??;
        }
        if std::env::var("APITAP_DEBUG").is_ok() {
            eprintln!(
                "[bq loader] rows={} files={n_jobs} transcode={:.1}s gzip={:.1}s \
                 upload={:.1}s job_wait={:.1}s",
                self.rows,
                self.t_transcode.as_secs_f64(),
                self.t_gzip.as_secs_f64(),
                self.t_upload.as_secs_f64(),
                t_poll.elapsed().as_secs_f64(),
            );
        }
        if committed != self.rows {
            return Err(Error::Transfer(format!(
                "bigquery load jobs landed {committed} rows, worker sent {} — \
                 refusing to continue on a partial load",
                self.rows
            )));
        }
        // Only rows the jobs COMMITTED may advance the staged watermark.
        if let Some(pq) = &mut self.pq {
            self.local_wm = pq.wm.take();
        }
        if let (Some((_, numeric)), Some(local)) = (self.cursor, self.local_wm.take()) {
            let mut shared = self.shared_wm.lock().expect("wm lock");
            *shared = wm_max(shared.take(), Some(local), numeric);
        }
        Ok(committed)
    }

    async fn abort(self, cause: Error) -> Error {
        // Sealed files' jobs may already have committed into STAGING — harmless,
        // staging never reaches the final table on a failed run and the next
        // run's prepare drops it. Just stop polling them.
        for handle in &self.pending_jobs {
            handle.abort();
        }
        // Cancel the resumable session so BigQuery DISCARDS the partial upload —
        // never finalize it into a load job. Google's cancel verb is a DELETE on
        // the session URI; best-effort, the session also just expires.
        if let Some(uri) = &self.session_uri {
            if let Ok(token) = self.conn.bearer().await {
                let _ = self.conn.client.delete(uri).bearer_auth(token).send().await;
            }
        }
        cause
    }
}

// ============================================================================
// Sink
// ============================================================================

pub(crate) struct BqSink {
    conn: BqConn,
    final_table: String,
    staging_table: String,
    job_config: Value,
    /// Worker stagings that actually received data (see BqLoader.staging_table).
    staging_registry: Arc<std::sync::Mutex<Vec<String>>>,
    loader_seq: std::sync::atomic::AtomicUsize,
    names: Vec<String>,
    parquet_lane: bool,
    /// Lane preference, decided at connect from the pipe count: Parquet's
    /// typed builders cost more CPU per row but upload less and parse fastest
    /// server-side — it wins from ~4 pipes up; the CSV lane wins on starved
    /// half-core boxes (measured on 1M/10M, see benchmarks/README).
    lane_order: [WireFormat; 2],
    delivered: Arc<Vec<Delivered>>,
    /// Incremental context for the state row (set in dest_state).
    source_id: Option<String>,
    cursor_col: Option<String>,
    mode_str: &'static str,
    /// (index, numeric) of the cursor column + the MAX the loaders observed —
    /// the staged watermark without a billed query against staging.
    cursor_track: Option<(usize, bool)>,
    staged_wm: Arc<std::sync::Mutex<Option<String>>>,
}

impl BqSink {
    pub(crate) async fn connect(url: &str, dest_table: &str, parallel: usize) -> Result<Self> {
        let conn = BqConn::parse(url).await?;
        let lane_order = if parallel >= 4 {
            [WireFormat::PgCopyBinary, WireFormat::TabSeparated]
        } else {
            [WireFormat::TabSeparated, WireFormat::PgCopyBinary]
        };
        let bare = match dest_table.rsplit_once('.') {
            Some((qual, t)) if qual == conn.dataset => t,
            Some((qual, _)) => {
                return Err(Error::InvalidInput(format!(
                    "dest_table qualifier '{qual}' doesn't match the url dataset \
                     '{}' — the dataset comes from the url",
                    conn.dataset
                )))
            }
            None => dest_table,
        };
        Ok(Self {
            conn,
            final_table: bare.to_string(),
            staging_table: format!("{bare}__apitap_staging"),
            job_config: Value::Null,
            staging_registry: Arc::new(std::sync::Mutex::new(Vec::new())),
            loader_seq: std::sync::atomic::AtomicUsize::new(0),
            names: Vec::new(),
            parquet_lane: false,
            lane_order,
            delivered: Arc::new(Vec::new()),
            source_id: None,
            cursor_col: None,
            mode_str: "replace",
            cursor_track: None,
            staged_wm: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    fn fq(&self, table: &str) -> String {
        format!("`{}.{}.{table}`", self.conn.project, self.conn.dataset)
    }

    async fn ensure_state_table(&self) -> Result<()> {
        if self.conn.table_get(STATE_TABLE).await?.is_some() {
            return Ok(());
        }
        let fields = json!([
            {"name": "dest_table", "type": "STRING", "mode": "REQUIRED"},
            {"name": "source_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "cursor_col", "type": "STRING", "mode": "NULLABLE"},
            {"name": "watermark", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mode", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_rows", "type": "INT64", "mode": "NULLABLE"},
            {"name": "synced_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]);
        match self.conn.table_create(STATE_TABLE, &fields).await {
            Ok(()) => Ok(()),
            // Two first-runs can race the CREATE; whoever loses appends fine.
            Err(Error::Transfer(m)) if m.contains("409") || m.contains("Already Exists") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// One NDJSON state row. `offset_micros` orders rows written by the same
    /// run (a replace's barrier must sort OLDER than its own state row).
    fn state_row(
        &self,
        source_id: &str,
        watermark: Option<&str>,
        mode: &str,
        rows: u64,
        offset_micros: i64,
    ) -> String {
        let ts = chrono::Utc::now() + chrono::Duration::microseconds(offset_micros);
        let mut row = json!({
            "dest_table": self.final_table,
            "source_id": source_id,
            "mode": mode,
            "last_rows": rows,
            "synced_at": ts.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
        });
        if let Some(wm) = watermark {
            row["watermark"] = json!(wm);
        }
        if let Some(c) = &self.cursor_col {
            row["cursor_col"] = json!(c);
        }
        let mut line = row.to_string();
        line.push('\n');
        line
    }

    /// Append this run's state row. The state table is APPEND-ONLY (load jobs
    /// only — DML is rejected on sandbox/free-tier projects); readers take the
    /// newest row per (dest_table, source_id), and a replace writes a `*`
    /// barrier row that invalidates everything older.
    async fn write_state(&self, watermark: &str, rows: u64) -> Result<()> {
        let Some(source_id) = self.source_id.clone() else {
            return Ok(());
        };
        self.ensure_state_table().await?;
        let row = self.state_row(&source_id, Some(watermark), self.mode_str, rows, 0);
        self.conn.load_rows(STATE_TABLE, row.into_bytes()).await
    }

    /// Replace destroyed every source's rows: a `*` barrier row supersedes all
    /// older state rows for this destination (the append-only DELETE).
    async fn write_replace_barrier(&self) -> Result<()> {
        self.ensure_state_table().await?;
        let row = self.state_row("*", None, "replace-barrier", 0, -1000);
        self.conn.load_rows(STATE_TABLE, row.into_bytes()).await
    }

    /// `MAX(cursor)` of `table` as text, `None` when the table has no rows.
    /// `expr` renders the max in the SOURCE's own text style (see
    /// `cursor_max_expr`) so it compares soundly against state rows.
    async fn max_cursor(&self, table: &str, expr: &str) -> Result<Option<String>> {
        let rows = self
            .conn
            .query(&format!("SELECT {expr} FROM {}", self.fq(table)))
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .flatten())
    }

    /// PG-style rendering of MAX(cursor): BigQuery's CAST puts 'T' in
    /// DATETIMEs and renders offsets differently — the state rows carry the
    /// source's rendering, and mixed formats would misorder lexicographic
    /// watermark comparisons (a re-read means DUPLICATES here; BigQuery
    /// tables don't dedup).
    fn cursor_max_expr(udt: &str, cursor: &str) -> String {
        let q = format!("`{cursor}`");
        match udt {
            "timestamptz" => {
                format!("FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S+00', MAX({q}), 'UTC')")
            }
            "timestamp" => format!("FORMAT_DATETIME('%Y-%m-%d %H:%M:%E*S', MAX({q}))"),
            _ => format!("CAST(MAX({q}) AS STRING)"),
        }
    }

    async fn copy_stagings_into_final(&self, sources: &[String], disposition: &str) -> Result<()> {
        let source_tables: Vec<Value> = sources
            .iter()
            .map(|t| {
                json!({
                    "projectId": self.conn.project, "datasetId": self.conn.dataset,
                    "tableId": t,
                })
            })
            .collect();
        let body = json!({
            "configuration": { "copy": {
                "sourceTables": source_tables,
                "destinationTable": {
                    "projectId": self.conn.project, "datasetId": self.conn.dataset,
                    "tableId": self.final_table,
                },
                "writeDisposition": disposition,
                "createDisposition": "CREATE_IF_NEEDED",
            }}
        });
        let v = self
            .conn
            .api(
                reqwest::Method::POST,
                format!("{BQ_BASE}/projects/{}/jobs", self.conn.project),
                Some(&body),
            )
            .await?;
        let job_id = v["jobReference"]["jobId"]
            .as_str()
            .ok_or_else(|| Error::Transfer("bigquery copy job missing jobId".into()))?
            .to_string();
        let loc = v["jobReference"]["location"].as_str().map(str::to_string);
        self.conn
            .poll_job(&job_id, loc.as_deref())
            .await
            .map(|_| ())
    }
}

impl crate::driver::Sink for BqSink {
    type Loader = BqLoader;

    fn accepts(&self) -> &[WireFormat] {
        &self.lane_order
    }

    fn adjust_plan(&self, _plan: &mut TablePlan) {}

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        _mode: Mode,
    ) -> Result<()> {
        self.conn.ensure_dataset().await?;
        self.parquet_lane = lane.format == WireFormat::PgCopyBinary;
        let mut fields = Vec::new();
        let mut delivered = Vec::new();
        let mut names = Vec::new();
        for (c, lc) in plan.cols.iter().zip(lane.cols.iter()) {
            check_col_name(&c.name)?;
            if self.parquet_lane && !crate::bqparquet::parquet_decodable(&c.udt) {
                return Err(Error::InvalidInput(format!(
                    "column {} has type {} — the binary lane can't decode it; \
                     cast it in a source view (e.g. {}::text)",
                    c.name, c.udt, c.name
                )));
            }
            // BigQuery rejects Parquet loads into JSON-typed columns outright
            // ("Unsupported field type: JSON") — the binary lane lands json/jsonb
            // as STRING (still valid JSON text; PARSE_JSON works on read).
            let bq_type = if self.parquet_lane && lc.delivered == Delivered::Json {
                "STRING"
            } else {
                bq_type_of(&lc.delivered)
            };
            fields.push(json!({
                "name": c.name, "type": bq_type, "mode": "NULLABLE",
            }));
            names.push(c.name.clone());
            delivered.push(lc.delivered.clone());
        }
        self.names = names;
        // Sweep every leftover worker staging (crashed runs included).
        for t in self.conn.tables_with_prefix(&self.staging_table).await? {
            self.conn.table_delete(&t).await?;
        }
        if let Some(cursor) = plan.cursor.as_deref() {
            if let Some(idx) = plan.cols.iter().position(|c| c.name == cursor) {
                let numeric = matches!(
                    lane.cols[idx].delivered,
                    Delivered::Int { .. } | Delivered::Decimal { .. }
                );
                self.cursor_track = Some((idx, numeric));
            }
        }
        self.delivered = Arc::new(delivered);
        self.job_config = json!({
            "configuration": { "load": {
                "destinationTable": {
                    "projectId": self.conn.project, "datasetId": self.conn.dataset,
                    "tableId": Value::Null, // per-loader (open() fills it)
                },
                // First job per worker CREATES its own staging with this schema.
                "schema": { "fields": fields },
                "createDisposition": "CREATE_IF_NEEDED",
                "sourceFormat": if self.parquet_lane { "PARQUET" } else { "CSV" },
                // CSV only: quoted fields may carry embedded newlines.
                "allowQuotedNewlines": !self.parquet_lane,
                "writeDisposition": "WRITE_APPEND",
                "maxBadRecords": 0,
            }}
        });
        Ok(())
    }

    async fn loader(&self) -> Result<BqLoader> {
        let i = self
            .loader_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        BqLoader::open(
            self.conn.clone(),
            &self.job_config,
            format!("{}_{i}", self.staging_table),
            self.staging_registry.clone(),
            &self.names,
            self.delivered.clone(),
            self.parquet_lane,
            self.cursor_track,
            self.staged_wm.clone(),
        )
    }

    async fn rows_staged(&self, loaded: u64) -> Result<u64> {
        // Load jobs report outputRows and each loader verified its own count.
        Ok(loaded)
    }

    async fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> Result<DestState> {
        if mode == Mode::Merge {
            return Err(Error::InvalidInput(
                "merge is not supported for BigQuery destinations yet — use append \
                 (BigQuery load+copy jobs are free; MERGE DML is planned)"
                    .into(),
            ));
        }
        self.source_id = Some(source_id.to_string());
        self.cursor_col = Some(cursor.to_string());
        self.mode_str = "append";
        let Some(meta) = self.conn.table_get(&self.final_table).await? else {
            return Ok(DestState {
                exists: false,
                watermark: None,
            });
        };
        let dest_cols: Vec<String> = meta["schema"]["fields"]
            .as_array()
            .map(|fs| {
                fs.iter()
                    .filter_map(|f| f["name"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let src_cols: Vec<String> = plan.cols.iter().map(|c| c.name.clone()).collect();
        if dest_cols != src_cols {
            return Err(Error::InvalidInput(format!(
                "destination columns {dest_cols:?} don't match the source {src_cols:?} — \
                 run once with mode='replace' to realign the schema"
            )));
        }
        // An emptied table cannot carry a watermark — resync must work. Query
        // MAX(cursor) directly (table metadata like numRows can lag behind jobs);
        // NULL means empty (or an all-NULL cursor, same conclusion).
        let max_expr = Self::cursor_max_expr(
            plan.cols
                .iter()
                .find(|c| c.name == cursor)
                .map(|c| c.udt.as_str())
                .unwrap_or(""),
            cursor,
        );
        let data_wm = self.max_cursor(&self.final_table, &max_expr).await?;
        if data_wm.is_none() {
            return Ok(DestState {
                exists: true,
                watermark: None,
            });
        }
        // The state table is append-only: take MY newest row and check for other
        // sources' rows, both relative to the newest `*` replace-barrier — all
        // resolved server-side in one free SELECT (timestamps never round-trip
        // through text).
        let (own_state, siblings) = if self.conn.table_get(STATE_TABLE).await?.is_some() {
            let sql = format!(
                "WITH s AS (SELECT * FROM {state} WHERE dest_table = '{dt}'), \
                 b AS (SELECT IFNULL(MAX(synced_at), TIMESTAMP '1970-01-01') AS ts \
                       FROM s WHERE source_id = '*') \
                 SELECT \
                   (SELECT watermark FROM s, b \
                    WHERE source_id = '{sid}' AND synced_at > b.ts \
                    ORDER BY synced_at DESC LIMIT 1), \
                   (SELECT CAST(COUNT(*) > 0 AS STRING) FROM s, b \
                    WHERE source_id NOT IN ('*', '{sid}') AND synced_at > b.ts)",
                state = self.fq(STATE_TABLE),
                dt = sql_str(&self.final_table),
                sid = sql_str(source_id),
            );
            let rows = self.conn.query(&sql).await?;
            let row = rows.into_iter().next().unwrap_or_default();
            let own = row.first().cloned().flatten();
            let sib = row.get(1).cloned().flatten().as_deref() == Some("true");
            (own, sib)
        } else {
            (None, false)
        };
        if own_state.is_none() && siblings {
            return Err(Error::InvalidInput(format!(
                "destination {} is fed by other sources (fan-in) but has no state row \
                 for THIS source — appending on the shared data watermark would skip \
                 rows; run once with mode='replace' or clean `_apitap_state`",
                self.final_table
            )));
        }
        let watermark = match (own_state, data_wm) {
            (Some(s), Some(d)) => {
                // Numeric when both parse; a numeric data max with garbage state
                // takes the data (bounded re-read, never a skip); two text values
                // (timestamps render sortably) compare lexicographically.
                Some(
                    match (s.parse::<i128>().is_ok(), d.parse::<i128>().is_ok()) {
                        (true, true) => wm_pick(true, s, d),
                        (true, false) => s,
                        (false, true) => d,
                        (false, false) => wm_pick(false, s, d),
                    },
                )
            }
            (Some(s), None) => Some(s),
            (None, d) => d,
        };
        Ok(DestState {
            exists: true,
            watermark,
        })
    }

    async fn finalize(&self, rows: u64, mode: Mode) -> Result<()> {
        let stagings: Vec<String> = self.staging_registry.lock().expect("registry lock").clone();
        if rows == 0 {
            for t in &stagings {
                self.conn.table_delete(t).await?;
            }
            return Ok(());
        }
        // The loaders tracked MAX(cursor) while transcoding — no billed query.
        let staged_wm = self.staged_wm.lock().expect("wm lock").clone();
        let t_fin = std::time::Instant::now();
        match mode {
            Mode::Replace => {
                self.copy_stagings_into_final(&stagings, "WRITE_TRUNCATE")
                    .await?;
                if std::env::var("APITAP_DEBUG").is_ok() {
                    eprintln!("[bq finalize] copy={:.1}s", t_fin.elapsed().as_secs_f64());
                }
                for t in &stagings {
                    self.conn.table_delete(t).await?;
                }
                // Replace destroyed every source's rows — supersede ALL stale
                // state rows for this destination, EVEN on a plain replace
                // (dest_state never ran, but a previous append's watermark would
                // make the next append skip everything below it). Append-only:
                // a `*` barrier row instead of a DELETE, because DML is rejected
                // on free-tier projects.
                if self.conn.table_get(STATE_TABLE).await?.is_some() || self.source_id.is_some() {
                    self.write_replace_barrier().await?;
                }
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                if std::env::var("APITAP_DEBUG").is_ok() {
                    eprintln!("[bq finalize] total={:.1}s", t_fin.elapsed().as_secs_f64());
                }
                Ok(())
            }
            Mode::Append => {
                self.copy_stagings_into_final(&stagings, "WRITE_APPEND")
                    .await?;
                for t in &stagings {
                    self.conn.table_delete(t).await?;
                }
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                Ok(())
            }
            Mode::Merge => Err(Error::InvalidInput(
                "merge is not supported for BigQuery destinations yet".into(),
            )),
        }
    }
}

/// Effective watermark: the freshest of the state row and the data max.
/// Numeric cursors compare numerically; text (timestamps render sortably)
/// lexicographically. An unparseable state value loses to the data max —
/// a bounded re-read, never a skip.
fn wm_pick(numeric: bool, state: String, data: String) -> String {
    if numeric {
        match (state.parse::<i128>(), data.parse::<i128>()) {
            (Ok(s), Ok(d)) => {
                if s >= d {
                    state
                } else {
                    data
                }
            }
            (Ok(_), Err(_)) => state,
            _ => data,
        }
    } else if state >= data {
        state
    } else {
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(d: Delivered, raw: &[u8]) -> String {
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        append_csv_value(raw, &d, &mut out, &mut scratch);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn csv_values_per_type() {
        assert_eq!(
            t(
                Delivered::Int {
                    bytes: 8,
                    unsigned: false
                },
                b"42"
            ),
            "42"
        );
        assert_eq!(t(Delivered::Bool, b"t"), "true");
        assert_eq!(t(Delivered::Bool, b"f"), "false");
        assert_eq!(t(Delivered::Float64, b"1.5"), "1.5");
        assert_eq!(t(Delivered::Float64, b"NaN"), "NaN");
        assert_eq!(
            t(Delivered::Decimal { p: 18, s: 4 }, b"12345.6789"),
            "12345.6789"
        );
        assert_eq!(
            t(Delivered::DateTime { utc: true }, b"2026-01-01 00:00:00+00"),
            "2026-01-01 00:00:00+00:00"
        );
        assert_eq!(
            t(
                Delivered::DateTime { utc: true },
                b"2026-01-01 00:00:00+05:30"
            ),
            "2026-01-01 00:00:00+05:30"
        );
        // text: raw when clean, quoted when empty or holding specials
        assert_eq!(t(Delivered::Text, b"hello"), "hello");
        assert_eq!(t(Delivered::Text, b""), "\"\""); // empty string, NOT NULL
        assert_eq!(t(Delivered::Text, b"a,b"), "\"a,b\"");
        assert_eq!(t(Delivered::Text, b"say \"hi\""), "\"say \"\"hi\"\"\"");
        // TSV-escaped newline unescapes, then rides inside quotes
        assert_eq!(t(Delivered::Text, b"a\\nb"), "\"a\nb\"");
        assert_eq!(t(Delivered::Json, br#"{"a": 1}"#), "\"{\"\"a\"\": 1}\"");
        assert_eq!(t(Delivered::Bytes, b"\\\\x4869"), "SGk=");
    }

    #[test]
    fn tsv_rows_to_csv_with_null_vs_empty() {
        let delivered = vec![
            Delivered::Int {
                bytes: 8,
                unsigned: false,
            },
            Delivered::Text,
        ];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        let mut wm = None;
        let rows = tsv_to_csv(
            b"1\thello\n2\t\\N\n3\t\n",
            &delivered,
            None,
            &mut wm,
            &mut out,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(rows, 3);
        // row 2: NULL -> unquoted empty; row 3: empty string -> quoted ""
        assert_eq!(String::from_utf8(out).unwrap(), "1,hello\n2,\n3,\"\"\n");
    }

    #[test]
    fn empty_line_is_a_real_row_for_single_text_column() {
        let delivered = vec![Delivered::Text];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        let mut wm = None;
        let rows = tsv_to_csv(
            b"a\n\nb\n",
            &delivered,
            None,
            &mut wm,
            &mut out,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(String::from_utf8(out).unwrap(), "a\n\"\"\nb\n");
    }

    #[test]
    fn tsv_field_count_mismatch_is_loud() {
        let delivered = vec![Delivered::Int {
            bytes: 8,
            unsigned: false,
        }];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        let mut wm = None;
        assert!(tsv_to_csv(b"1\t2\n", &delivered, None, &mut wm, &mut out, &mut scratch).is_err());
        assert!(tsv_to_csv(b"", &delivered, None, &mut wm, &mut out, &mut scratch).is_ok());
    }

    #[test]
    fn loader_tracks_cursor_max_during_transcode() {
        let delivered = vec![
            Delivered::Int {
                bytes: 8,
                unsigned: false,
            },
            Delivered::Text,
        ];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        let mut wm = None;
        tsv_to_csv(
            b"9\ta\n100\tb\n21\tc\n",
            &delivered,
            Some((0, true)),
            &mut wm,
            &mut out,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(wm.as_deref(), Some("100")); // numeric, not lexicographic
        let mut wm2 = None;
        tsv_to_csv(
            b"\\N\tx\n",
            &delivered,
            Some((0, true)),
            &mut wm2,
            &mut out,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(wm2, None);
    }

    #[test]
    fn bq_types_map_losslessly() {
        assert_eq!(
            bq_type_of(&Delivered::Int {
                bytes: 8,
                unsigned: true
            }),
            "NUMERIC"
        );
        assert_eq!(
            bq_type_of(&Delivered::Int {
                bytes: 8,
                unsigned: false
            }),
            "INT64"
        );
        assert_eq!(bq_type_of(&Delivered::Decimal { p: 18, s: 4 }), "NUMERIC");
        // NUMERIC holds <=29 integer digits at scale <=9 - (38,9) fits exactly.
        assert_eq!(bq_type_of(&Delivered::Decimal { p: 38, s: 9 }), "NUMERIC");
        assert_eq!(
            bq_type_of(&Delivered::Decimal { p: 40, s: 10 }),
            "BIGNUMERIC"
        );
        assert_eq!(bq_type_of(&Delivered::Decimal { p: 76, s: 30 }), "STRING");
        assert_eq!(
            bq_type_of(&Delivered::Decimal { p: 65, s: 30 }),
            "BIGNUMERIC"
        );
        assert_eq!(bq_type_of(&Delivered::DateTime { utc: true }), "TIMESTAMP");
        assert_eq!(bq_type_of(&Delivered::DateTime { utc: false }), "DATETIME");
        assert_eq!(bq_type_of(&Delivered::Json), "JSON");
    }

    #[test]
    fn base64_and_hex_roundtrip() {
        let mut out = Vec::new();
        base64_into(b"Man", &mut out);
        assert_eq!(out, b"TWFu");
        out.clear();
        base64_into(b"Ma", &mut out);
        assert_eq!(out, b"TWE=");
        out.clear();
        base64_into(b"M", &mut out);
        assert_eq!(out, b"TQ==");
    }

    #[test]
    fn column_names_validated_not_renamed() {
        assert!(check_col_name("valid_name1").is_ok());
        assert!(check_col_name("_leading").is_ok());
        assert!(check_col_name("1digit").is_err());
        assert!(check_col_name("has space").is_err());
        assert!(check_col_name("has-dash").is_err());
    }

    #[test]
    fn cursor_max_renders_in_source_style() {
        assert!(BqSink::cursor_max_expr("timestamptz", "ts").contains("FORMAT_TIMESTAMP"));
        assert!(BqSink::cursor_max_expr("timestamp", "ts").contains("FORMAT_DATETIME"));
        assert!(BqSink::cursor_max_expr("int8", "id").starts_with("CAST(MAX"));
    }

    #[test]
    fn wm_pick_never_skips() {
        assert_eq!(wm_pick(true, "1000".into(), "500".into()), "1000");
        assert_eq!(wm_pick(true, "500".into(), "1000".into()), "1000");
        assert_eq!(wm_pick(true, "garbage".into(), "500".into()), "500");
        assert_eq!(
            wm_pick(
                false,
                "2026-01-02 00:00:00+00".into(),
                "2026-01-01 00:00:00+00".into()
            ),
            "2026-01-02 00:00:00+00"
        );
    }

    #[test]
    fn url_requires_project_and_dataset() {
        // parse() is async because of the token exchange; check the URL part
        // synchronously through the error paths of a blocking runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for bad in [
            "bigquery://",
            "bigquery://proj",
            "bigquery://proj/ds?nope=1",
        ] {
            assert!(rt.block_on(BqConn::parse(bad)).is_err(), "{bad}");
        }
    }
}
