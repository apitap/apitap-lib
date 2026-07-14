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
        let b = field[i];
        if b == b'\\' && i + 1 < field.len() {
            i += 1;
            out.push(match field[i] {
                b'b' => 0x08,
                b'f' => 0x0c,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'v' => 0x0b,
                other => other, // covers \\ and any literal escape
            });
        } else {
            out.push(b);
        }
        i += 1;
    }
}

fn json_escape_into(s: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    for &b in s {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            c if c < 0x20 => {
                out.extend_from_slice(format!("\\u{c:04x}").as_bytes());
            }
            c => out.push(c),
        }
    }
    out.push(b'"');
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

/// Append one field's JSON value for `d`. `raw` is the still-escaped TSV field.
fn append_json_value(raw: &[u8], d: &Delivered, out: &mut Vec<u8>, scratch: &mut Vec<u8>) {
    scratch.clear();
    let val: &[u8] = if raw.contains(&b'\\') {
        unescape_into(raw, scratch);
        scratch.as_slice()
    } else {
        raw
    };
    match d {
        // Exact wide types go QUOTED: BigQuery coerces the string to
        // NUMERIC/BIGNUMERIC exactly, while a bare JSON number risks a
        // double-precision round-trip. Signed ints are exact either way.
        Delivered::Int {
            bytes: 8,
            unsigned: true,
        }
        | Delivered::Decimal { .. } => {
            json_escape_into(val, out); // also covers numeric NaN
        }
        Delivered::Int { .. } => out.extend_from_slice(val),
        Delivered::Float32 | Delivered::Float64 => {
            // NaN/Infinity aren't JSON numbers; BigQuery accepts them quoted.
            if val.first().is_some_and(|c| c.is_ascii_alphabetic())
                || val.get(1).is_some_and(|c| c.is_ascii_alphabetic())
            {
                json_escape_into(val, out);
            } else {
                out.extend_from_slice(val);
            }
        }
        Delivered::Bool => out.extend_from_slice(if val == b"t" { b"true" } else { b"false" }),
        Delivered::DateTime { utc: true } => {
            // Postgres (UTC session) renders "…+00"; BigQuery wants a full offset.
            out.push(b'"');
            out.extend_from_slice(val);
            if val.len() > 3 && matches!(val[val.len() - 3], b'+' | b'-') {
                out.extend_from_slice(b":00");
            }
            out.push(b'"');
        }
        Delivered::Date | Delivered::DateTime { utc: false } | Delivered::Uuid => {
            out.push(b'"');
            out.extend_from_slice(val);
            out.push(b'"');
        }
        // jsonb/json text from the source is valid JSON by construction — embed
        // raw. `json` (not jsonb) preserves stored formatting, so pretty-printed
        // values carry literal newlines that would split the NDJSON record; in
        // valid JSON a raw LF/CR can only be inter-token whitespace (PG rejects
        // raw control chars inside strings), so a space is lossless.
        Delivered::Json => {
            if val.iter().any(|&b| matches!(b, b'\n' | b'\r')) {
                out.extend(
                    val.iter()
                        .map(|&b| if matches!(b, b'\n' | b'\r') { b' ' } else { b }),
                );
            } else {
                out.extend_from_slice(val);
            }
        }
        Delivered::Text => json_escape_into(val, out),
        Delivered::Bytes => {
            // bytea_output=hex: "\x<hex>" (already unescaped to `\x…` here).
            let hex = val.strip_prefix(b"\\x").unwrap_or(val);
            let bytes: Vec<u8> = hex
                .chunks(2)
                .map(|p| (hex_val(p[0]) << 4) | p.get(1).map(|&b| hex_val(b)).unwrap_or(0))
                .collect();
            out.push(b'"');
            base64_into(&bytes, out);
            out.push(b'"');
        }
    }
}

/// Transcode record-aligned TSV bytes into NDJSON. Returns rows converted.
/// NULL fields are omitted entirely (missing key = NULL in BigQuery).
fn tsv_to_ndjson(
    input: &[u8],
    keys: &[Vec<u8>],
    delivered: &[Delivered],
    out: &mut Vec<u8>,
    scratch: &mut Vec<u8>,
) -> Result<u64> {
    let mut rows = 0u64;
    // split_inclusive: no trailing artifact to skip, so a genuinely empty line
    // (a single empty-string text column) stays a REAL record.
    for piece in input.split_inclusive(|&b| b == b'\n') {
        let line = piece.strip_suffix(b"\n").unwrap_or(piece);
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        out.push(b'{');
        let mut first = true;
        let mut col = 0usize;
        for field in line.split(|&b| b == b'\t') {
            if col >= keys.len() {
                return Err(Error::Transfer(format!(
                    "tsv row has more fields than the {} planned columns",
                    keys.len()
                )));
            }
            if field != b"\\N" {
                if !first {
                    out.push(b',');
                }
                first = false;
                out.extend_from_slice(&keys[col]);
                append_json_value(field, &delivered[col], out, scratch);
            }
            col += 1;
        }
        if col != keys.len() {
            return Err(Error::Transfer(format!(
                "tsv row has {col} fields, expected {}",
                keys.len()
            )));
        }
        out.extend_from_slice(b"}\n");
        rows += 1;
    }
    Ok(rows)
}

// ============================================================================
// Loader: one resumable-upload load job per worker
// ============================================================================

pub(crate) struct BqLoader {
    conn: BqConn,
    job_config: Arc<Value>,
    keys: Arc<Vec<Vec<u8>>>,
    delivered: Arc<Vec<Delivered>>,
    gz: flate2::write::GzEncoder<Vec<u8>>,
    session_uri: Option<String>,
    offset: u64,
    rows: u64,
    ndjson: Vec<u8>,
    scratch: Vec<u8>,
}

impl BqLoader {
    fn open(
        conn: BqConn,
        job_config: Arc<Value>,
        keys: Arc<Vec<Vec<u8>>>,
        delivered: Arc<Vec<Delivered>>,
    ) -> Self {
        Self {
            conn,
            job_config,
            keys,
            delivered,
            gz: flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast()),
            session_uri: None,
            offset: 0,
            rows: 0,
            ndjson: Vec::new(),
            scratch: Vec::new(),
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
            .json(self.job_config.as_ref())
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

    /// Ship every full 256 KiB-aligned span the gzip buffer holds.
    async fn drain_aligned(&mut self) -> Result<()> {
        loop {
            let ready = self.gz.get_ref().len();
            if ready < UPLOAD_CHUNK {
                return Ok(());
            }
            let take = (ready / UPLOAD_ALIGN) * UPLOAD_ALIGN;
            let chunk: Vec<u8> = self.gz.get_mut().drain(..take).collect();
            self.put_chunk(chunk, None).await?;
        }
    }
}

impl Loader for BqLoader {
    async fn send(&mut self, buf: Vec<u8>) -> Result<()> {
        use std::io::Write;
        self.ndjson.clear();
        self.rows += tsv_to_ndjson(
            &buf,
            &self.keys,
            &self.delivered,
            &mut self.ndjson,
            &mut self.scratch,
        )?;
        self.gz
            .write_all(&self.ndjson)
            .map_err(|e| Error::Transfer(format!("gzip encode: {e}")))?;
        self.drain_aligned().await
    }

    async fn finish(mut self) -> Result<u64> {
        if self.rows == 0 && self.session_uri.is_none() {
            return Ok(0); // nothing streamed: no load job at all
        }
        let tail = std::mem::replace(&mut self.gz, {
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast())
        })
        .finish()
        .map_err(|e| Error::Transfer(format!("gzip finish: {e}")))?;
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
        let done = self.conn.poll_job(&job_id, location.as_deref()).await?;
        let out_rows: u64 = done["statistics"]["load"]["outputRows"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if out_rows != self.rows {
            return Err(Error::Transfer(format!(
                "bigquery load job {job_id} landed {out_rows} rows, worker sent {} — \
                 refusing to continue on a partial load",
                self.rows
            )));
        }
        Ok(out_rows)
    }

    async fn abort(self, cause: Error) -> Error {
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
    job_config: Arc<Value>,
    keys: Arc<Vec<Vec<u8>>>,
    delivered: Arc<Vec<Delivered>>,
    /// Incremental context for the state row (set in dest_state).
    source_id: Option<String>,
    cursor_col: Option<String>,
    mode_str: &'static str,
}

impl BqSink {
    pub(crate) async fn connect(url: &str, dest_table: &str) -> Result<Self> {
        let conn = BqConn::parse(url).await?;
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
            job_config: Arc::new(Value::Null),
            keys: Arc::new(Vec::new()),
            delivered: Arc::new(Vec::new()),
            source_id: None,
            cursor_col: None,
            mode_str: "replace",
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

    async fn write_state(&self, watermark: &str, rows: u64) -> Result<()> {
        self.ensure_state_table().await?;
        let (Some(source_id), Some(cursor)) = (&self.source_id, &self.cursor_col) else {
            return Ok(());
        };
        let sql = format!(
            "MERGE {state} T USING (SELECT '{dt}' AS dest_table, '{sid}' AS source_id) S \
             ON T.dest_table = S.dest_table AND T.source_id = S.source_id \
             WHEN MATCHED THEN UPDATE SET cursor_col = '{cur}', watermark = '{wm}', \
               mode = '{md}', last_rows = {rows}, synced_at = CURRENT_TIMESTAMP() \
             WHEN NOT MATCHED THEN INSERT \
               (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) \
             VALUES ('{dt}', '{sid}', '{cur}', '{wm}', '{md}', {rows}, CURRENT_TIMESTAMP())",
            state = self.fq(STATE_TABLE),
            dt = sql_str(&self.final_table),
            sid = sql_str(source_id),
            cur = sql_str(cursor),
            wm = sql_str(watermark),
            md = self.mode_str,
        );
        self.conn.query(&sql).await.map(|_| ())
    }

    /// `MAX(cursor)` of `table` as text, `None` when the table has no rows.
    async fn max_cursor(&self, table: &str, cursor: &str) -> Result<Option<String>> {
        let rows = self
            .conn
            .query(&format!(
                "SELECT CAST(MAX(`{cursor}`) AS STRING) FROM {}",
                self.fq(table)
            ))
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .flatten())
    }

    async fn copy_staging_into_final(&self, disposition: &str) -> Result<()> {
        let body = json!({
            "configuration": { "copy": {
                "sourceTable": {
                    "projectId": self.conn.project, "datasetId": self.conn.dataset,
                    "tableId": self.staging_table,
                },
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

    fn accepts(&self) -> &'static [WireFormat] {
        &[WireFormat::TabSeparated]
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
        let mut fields = Vec::new();
        let mut keys = Vec::new();
        let mut delivered = Vec::new();
        for (c, lc) in plan.cols.iter().zip(lane.cols.iter()) {
            check_col_name(&c.name)?;
            fields.push(json!({
                "name": c.name, "type": bq_type_of(&lc.delivered), "mode": "NULLABLE",
            }));
            keys.push(format!("\"{}\":", c.name).into_bytes());
            delivered.push(lc.delivered.clone());
        }
        self.conn.table_delete(&self.staging_table).await?;
        self.conn
            .table_create(&self.staging_table, &Value::Array(fields))
            .await?;
        self.keys = Arc::new(keys);
        self.delivered = Arc::new(delivered);
        self.job_config = Arc::new(json!({
            "configuration": { "load": {
                "destinationTable": {
                    "projectId": self.conn.project, "datasetId": self.conn.dataset,
                    "tableId": self.staging_table,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "maxBadRecords": 0,
            }}
        }));
        Ok(())
    }

    async fn loader(&self) -> Result<BqLoader> {
        Ok(BqLoader::open(
            self.conn.clone(),
            self.job_config.clone(),
            self.keys.clone(),
            self.delivered.clone(),
        ))
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
        let data_wm = self.max_cursor(&self.final_table, cursor).await?;
        if data_wm.is_none() {
            return Ok(DestState {
                exists: true,
                watermark: None,
            });
        }
        let state_rows = if self.conn.table_get(STATE_TABLE).await?.is_some() {
            self.conn
                .query(&format!(
                    "SELECT watermark, source_id FROM {} WHERE dest_table = '{}'",
                    self.fq(STATE_TABLE),
                    sql_str(&self.final_table)
                ))
                .await?
        } else {
            Vec::new()
        };
        let own_state = state_rows
            .iter()
            .find(|r| r.get(1).and_then(|v| v.as_deref()) == Some(source_id))
            .and_then(|r| r.first().cloned())
            .flatten();
        if own_state.is_none() && !state_rows.is_empty() {
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
        if rows == 0 {
            return self.conn.table_delete(&self.staging_table).await;
        }
        let staged_wm = match &self.cursor_col {
            Some(c) => self.max_cursor(&self.staging_table, c).await?,
            None => None,
        };
        match mode {
            Mode::Replace => {
                self.copy_staging_into_final("WRITE_TRUNCATE").await?;
                self.conn.table_delete(&self.staging_table).await?;
                // Replace destroyed every source's rows — clear ALL stale state
                // rows for this destination, EVEN on a plain replace (dest_state
                // never ran, but a previous append's watermark would make the
                // next append skip everything below it).
                if self.conn.table_get(STATE_TABLE).await?.is_some() {
                    self.conn
                        .query(&format!(
                            "DELETE FROM {} WHERE dest_table = '{}'",
                            self.fq(STATE_TABLE),
                            sql_str(&self.final_table)
                        ))
                        .await?;
                }
                if let Some(wm) = &staged_wm {
                    self.write_state(wm, rows).await?;
                }
                Ok(())
            }
            Mode::Append => {
                self.copy_staging_into_final("WRITE_APPEND").await?;
                self.conn.table_delete(&self.staging_table).await?;
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
        append_json_value(raw, &d, &mut out, &mut scratch);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn json_values_per_type() {
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
        assert_eq!(t(Delivered::Float64, b"NaN"), "\"NaN\"");
        assert_eq!(t(Delivered::Float64, b"-Infinity"), "\"-Infinity\"");
        assert_eq!(
            t(Delivered::DateTime { utc: true }, b"2026-01-01 00:00:00+00"),
            "\"2026-01-01 00:00:00+00:00\""
        );
        assert_eq!(
            t(
                Delivered::DateTime { utc: true },
                b"2026-01-01 00:00:00+05:30"
            ),
            "\"2026-01-01 00:00:00+05:30\""
        );
        assert_eq!(
            t(Delivered::DateTime { utc: false }, b"2026-01-01 00:00:00"),
            "\"2026-01-01 00:00:00\""
        );
        assert_eq!(t(Delivered::Text, b"a\\tb"), "\"a\\tb\"");
        assert_eq!(t(Delivered::Json, br#"{"a": 1}"#), r#"{"a": 1}"#);
        // exact wide types ride as strings (BigQuery coerces exactly)
        assert_eq!(
            t(Delivered::Decimal { p: 18, s: 4 }, b"12345.6789"),
            "\"12345.6789\""
        );
        assert_eq!(t(Delivered::Decimal { p: 18, s: 4 }, b"NaN"), "\"NaN\"");
        assert_eq!(
            t(
                Delivered::Int {
                    bytes: 8,
                    unsigned: true
                },
                b"18446744073709551615"
            ),
            "\"18446744073709551615\""
        );
        assert_eq!(t(Delivered::Bytes, b"\\\\x4869"), "\"SGk=\"");
    }

    #[test]
    fn tsv_rows_to_ndjson_with_null_omission() {
        let keys = vec![b"\"id\":".to_vec(), b"\"name\":".to_vec()];
        let delivered = vec![
            Delivered::Int {
                bytes: 8,
                unsigned: false,
            },
            Delivered::Text,
        ];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        let rows = tsv_to_ndjson(
            b"1\thello\n2\t\\N\n",
            &keys,
            &delivered,
            &mut out,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"id\":1,\"name\":\"hello\"}\n{\"id\":2}\n"
        );
    }

    #[test]
    fn empty_line_is_a_real_row_for_single_text_column() {
        // "\n" alone = one row whose only text column is the empty string —
        // it must NOT be conflated with the split artifact after the last row.
        let keys = vec![b"\"name\":".to_vec()];
        let delivered = vec![Delivered::Text];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        let rows = tsv_to_ndjson(b"a\n\nb\n", &keys, &delivered, &mut out, &mut scratch).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"name\":\"a\"}\n{\"name\":\"\"}\n{\"name\":\"b\"}\n"
        );
    }

    #[test]
    fn pg_json_embedded_newlines_stay_one_record() {
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        append_json_value(b"{\"a\":  1}", &Delivered::Json, &mut out, &mut scratch);
        // `json` columns keep stored formatting; raw newlines arrive TSV-escaped
        // as \n and unescape to real LF — they must not split the NDJSON record.
        out.clear();
        append_json_value(
            br#"{\n  \"a\": 1\n}"#,
            &Delivered::Json,
            &mut out,
            &mut scratch,
        );
        let s = String::from_utf8(out).unwrap();
        assert!(!s.contains('\n'), "{s:?}");
        assert_eq!(s, "{   \"a\": 1 }");
    }

    #[test]
    fn tsv_field_count_mismatch_is_loud() {
        let keys = vec![b"\"id\":".to_vec()];
        let delivered = vec![Delivered::Int {
            bytes: 8,
            unsigned: false,
        }];
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        assert!(tsv_to_ndjson(b"1\t2\n", &keys, &delivered, &mut out, &mut scratch).is_err());
        assert!(tsv_to_ndjson(b"", &keys, &delivered, &mut out, &mut scratch).is_ok());
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
