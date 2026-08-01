//! Apache Iceberg destination: `iceberg://<catalog-host[:port]>/<namespace>`
//! against any REST catalog (Lakekeeper, Polaris, Nessie, Glue REST, R2 Data
//! Catalog, S3 Tables …) with S3-compatible object storage underneath.
//!
//! All three modes are real Iceberg semantics, not emulation:
//!
//! - `replace` → an **overwrite snapshot**: the new manifest list carries only
//!   the fresh data manifests. Old snapshots (and time travel) stay intact —
//!   readers flip atomically at the catalog commit.
//! - `append` → an **append snapshot**: existing manifests are carried over,
//!   one new data manifest is added. The cursor watermark rides in a table
//!   property (`apitap.watermark.<source>`) **in the same commit** — state and
//!   data are transactional exactly like the Postgres `_apitap_state` story.
//! - `merge` → a **row-delta snapshot** (merge-on-read upsert): the delta's
//!   merge-key values become an equality-delete file at the new sequence
//!   number (killing older rows with those keys), and the delta lands as data
//!   files in the same snapshot — same-sequence data is exempt from the
//!   deletes by spec, which is precisely upsert.
//!
//! The commit protocol is hand-rolled REST (`POST …/tables/<t>` with
//! requirements + updates) over `iceberg-rust`'s spec primitives (schema,
//! Avro manifest/manifest-list writers, snapshot & update serde) — the crate's
//! own `Transaction` only ships `fast_append` today. Manifests are written
//! through an in-memory FileIO and PUT with the same SigV4 client the `s3://`
//! sink uses; data files stream through the identical multipart path.
//!
//! v1 scope, loudly enforced: format-version 2 tables, single-level
//! namespaces, single-column integer/text/uuid merge keys, storage the
//! catalog locates at `s3://…`. Snapshot expiry/compaction is table
//! maintenance and stays with the catalog/engine, not the transfer tool.

use crate::aws::read_credentials;
use crate::error::{Error, Result};
use crate::plan::{
    resolve_watermark, wm_max, Delivered, DestState, Lane, TablePlan, WireFormat, WmArbitration,
};
use crate::sink::s3::S3Conn;
use crate::wire::bqparquet::{parquet_col_ok, KeyCap, ParquetEncoder};
use crate::Mode;
use iceberg::io::{FileIO, FileWrite};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestFile,
    ManifestList, ManifestListWriter, ManifestWriterBuilder, NestedField, Operation, PrimitiveType,
    Schema, Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary, TableMetadata,
    Type as IceType, MAIN_BRANCH, UNASSIGNED_SEQUENCE_NUMBER,
};
use iceberg::{TableRequirement, TableUpdate};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SEND_THRESHOLD: usize = 8 * 1024 * 1024;

/// One finished data file, reported by its loader for the commit.
struct FileDone {
    /// Full `s3://…` URI (what goes in the manifest).
    path: String,
    /// Object key inside the bucket (what the cleanup path deletes).
    key: String,
    rows: u64,
    bytes: u64,
    /// Max cursor value seen, rendered source-style.
    wm: Option<String>,
    keys: KeyCap,
}

// ============================================================================
// Connection: URL parse + hand-rolled REST catalog client
// ============================================================================

#[derive(Clone)]
pub(crate) struct IcebergConn {
    http: reqwest::Client,
    /// `scheme://host[:port][/base]` — no trailing slash.
    base: String,
    /// Catalog-assigned path prefix from `GET /v1/config` ("" when absent).
    prefix: String,
    warehouse: Option<String>,
    token: Option<String>,
    pub(crate) namespace: String,
    s3_endpoint: Option<String>,
    region: String,
    creds: crate::aws::AwsCreds,
}

fn dec(s: &str) -> Result<String> {
    // Same contract as the s3/gcs URL decoders: percent-escapes, '+' literal.
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
                    Error::InvalidInput(format!("iceberg url: invalid percent-escape in '{s}'"))
                })?;
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|e| Error::InvalidInput(format!("iceberg url is not UTF-8: {e}")))
}

impl IcebergConn {
    pub(crate) async fn parse(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidInput(format!("iceberg url: {e}")))?;
        let host = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "iceberg url needs the REST catalog host: \
                     iceberg://<host[:port]>/<namespace>?warehouse=…\
                     [&base=catalog][&endpoint=…][&access_key_id=…&secret_access_key=…]"
                        .into(),
                )
            })?
            .to_string();
        let namespace = dec(u.path().trim_matches('/'))?;
        if namespace.is_empty() {
            return Err(Error::InvalidInput(
                "iceberg url needs a namespace: iceberg://host:port/<namespace>".into(),
            ));
        }
        if namespace.contains('.') || namespace.contains('/') {
            return Err(Error::InvalidInput(format!(
                "iceberg: multi-level namespace '{namespace}' isn't supported yet — \
                 use a single-level namespace"
            )));
        }
        let (mut warehouse, mut base, mut token, mut tls) = (None, None, None, None);
        let (mut endpoint, mut region) = (None, None);
        let (mut key_id, mut secret, mut session) = (None, None, None);
        for pair in u.query().unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let (k, v) = (dec(k)?, dec(v)?);
            match k.as_str() {
                "warehouse" => warehouse = Some(v),
                "base" => base = Some(v.trim_matches('/').to_string()),
                "token" => token = Some(v),
                "tls" => tls = Some(v == "1" || v == "true"),
                "endpoint" => endpoint = Some(v),
                "region" => region = Some(v),
                "access_key_id" => key_id = Some(v),
                "secret_access_key" => secret = Some(v),
                "session_token" => session = Some(v),
                other => {
                    return Err(Error::InvalidInput(format!(
                        "unknown iceberg url parameter '{other}' (supported: warehouse, base, \
                         token, tls, endpoint, region, access_key_id, secret_access_key, \
                         session_token)"
                    )))
                }
            }
        }
        // Loopback catalogs default to http (dev: Lakekeeper/MinIO on one box);
        // anything remote defaults to https. `tls=0/1` overrides either way.
        let local = host == "localhost"
            || host.starts_with("127.")
            || host == "[::1]"
            || host.starts_with("host.docker.internal");
        let scheme = match tls {
            Some(true) => "https",
            Some(false) => "http",
            None if local => "http",
            None => "https",
        };
        let port = u.port().map(|p| format!(":{p}")).unwrap_or_default();
        let base = match base {
            Some(b) if !b.is_empty() => format!("{scheme}://{host}{port}/{b}"),
            _ => format!("{scheme}://{host}{port}"),
        };
        let region = region
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        let creds = read_credentials(key_id, secret, session)?;
        let mut conn = Self {
            http: reqwest::Client::new(),
            base,
            prefix: String::new(),
            warehouse,
            token,
            namespace,
            s3_endpoint: endpoint,
            region,
            creds,
        };
        conn.fetch_config().await?;
        Ok(conn)
    }

    fn req(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        let mut r = self.http.request(method, url);
        if let Some(t) = &self.token {
            r = r.bearer_auth(t);
        }
        r
    }

    fn v1(&self, rest: &str) -> String {
        if self.prefix.is_empty() {
            format!("{}/v1/{rest}", self.base)
        } else {
            format!("{}/v1/{}/{rest}", self.base, self.prefix)
        }
    }

    async fn body_ok(r: reqwest::Response, what: &str) -> Result<serde_json::Value> {
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        if !status.is_success() {
            let mut excerpt = text;
            excerpt.truncate(500);
            return Err(Error::Transfer(format!("iceberg catalog {what}: {status} {excerpt}")));
        }
        serde_json::from_str(&text)
            .map_err(|e| Error::Transfer(format!("iceberg catalog {what}: bad JSON: {e}")))
    }

    /// `GET /v1/config` — resolves the path prefix some catalogs (Lakekeeper)
    /// put in front of every route.
    async fn fetch_config(&mut self) -> Result<()> {
        let mut url = format!("{}/v1/config", self.base);
        if let Some(w) = &self.warehouse {
            url.push_str(&format!("?warehouse={}", enc_q(w)));
        }
        let r = self
            .req(reqwest::Method::GET, url)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("iceberg catalog config: {e}")))?;
        let v = Self::body_ok(r, "config").await?;
        self.prefix = v["overrides"]["prefix"]
            .as_str()
            .or_else(|| v["defaults"]["prefix"].as_str())
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        Ok(())
    }

    async fn ensure_namespace(&self) -> Result<()> {
        let get = self
            .req(reqwest::Method::GET, self.v1(&format!("namespaces/{}", enc_q(&self.namespace))))
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("iceberg catalog: {e}")))?;
        if get.status().is_success() {
            return Ok(());
        }
        let r = self
            .req(reqwest::Method::POST, self.v1("namespaces"))
            .json(&serde_json::json!({"namespace": [self.namespace], "properties": {}}))
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("iceberg catalog: {e}")))?;
        // 409 = raced another creator; that is success for "ensure".
        if r.status().is_success() || r.status().as_u16() == 409 {
            Ok(())
        } else {
            Self::body_ok(r, "create namespace").await.map(|_| ())
        }
    }

    /// `None` when the table doesn't exist.
    async fn load_table(&self, table: &str) -> Result<Option<TableMetadata>> {
        let r = self
            .req(
                reqwest::Method::GET,
                self.v1(&format!("namespaces/{}/tables/{}", enc_q(&self.namespace), enc_q(table))),
            )
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("iceberg catalog: {e}")))?;
        if r.status().as_u16() == 404 {
            return Ok(None);
        }
        let v = Self::body_ok(r, "load table").await?;
        let meta: TableMetadata = serde_json::from_value(v["metadata"].clone())
            .map_err(|e| Error::Transfer(format!("iceberg: table metadata didn't parse: {e}")))?;
        Ok(Some(meta))
    }

    async fn create_table(&self, table: &str, schema: &Schema) -> Result<TableMetadata> {
        let body = serde_json::json!({
            "name": table,
            "schema": serde_json::to_value(schema)
                .map_err(|e| Error::Transfer(format!("iceberg: schema serialize: {e}")))?,
            "stage-create": false,
            "properties": {
                "write.parquet.compression-codec": "zstd",
                "created-by": "apitap",
            },
        });
        let r = self
            .req(
                reqwest::Method::POST,
                self.v1(&format!("namespaces/{}/tables", enc_q(&self.namespace))),
            )
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("iceberg catalog: {e}")))?;
        if r.status().as_u16() == 409 {
            // Raced another creator — load what won.
            return self.load_table(table).await?.ok_or_else(|| {
                Error::Transfer("iceberg: create raced but table then vanished".into())
            });
        }
        let v = Self::body_ok(r, "create table").await?;
        serde_json::from_value(v["metadata"].clone())
            .map_err(|e| Error::Transfer(format!("iceberg: created metadata didn't parse: {e}")))
    }

    /// One optimistic-concurrency commit attempt. `Ok(false)` = requirement
    /// conflict (someone else committed first — reload and retry).
    async fn commit_table(
        &self,
        table: &str,
        requirements: &[TableRequirement],
        updates: &[TableUpdate],
    ) -> Result<bool> {
        let body = serde_json::json!({
            "identifier": {"namespace": [self.namespace], "name": table},
            "requirements": requirements,
            "updates": updates,
        });
        let r = self
            .req(
                reqwest::Method::POST,
                self.v1(&format!("namespaces/{}/tables/{}", enc_q(&self.namespace), enc_q(table))),
            )
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("iceberg commit: {e}")))?;
        if matches!(r.status().as_u16(), 409 | 412) {
            return Ok(false);
        }
        Self::body_ok(r, "commit").await.map(|_| true)
    }
}

fn enc_q(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ============================================================================
// Type mapping + schema checks
// ============================================================================

/// The Iceberg type each [`Delivered`] lands as. Must stay in lockstep with
/// `parquet_field` — Iceberg readers resolve parquet columns BY FIELD ID and
/// then trust this declared type.
fn ice_type(d: &Delivered) -> IceType {
    match d {
        Delivered::Int { .. } => IceType::Primitive(PrimitiveType::Long),
        Delivered::Float32 => IceType::Primitive(PrimitiveType::Float),
        Delivered::Float64 => IceType::Primitive(PrimitiveType::Double),
        Delivered::Bool => IceType::Primitive(PrimitiveType::Boolean),
        Delivered::Decimal { p, s } => {
            let precision = if *p == 0 || *p > 38 { 38 } else { *p as u32 };
            let scale = (*s as u32).min(precision);
            IceType::Primitive(PrimitiveType::Decimal { precision, scale })
        }
        Delivered::Date => IceType::Primitive(PrimitiveType::Date),
        Delivered::DateTime { utc: true } => IceType::Primitive(PrimitiveType::Timestamptz),
        Delivered::DateTime { utc: false } => IceType::Primitive(PrimitiveType::Timestamp),
        // Parquet writes uuid/json as UTF-8 strings — declare what the bytes are.
        Delivered::Uuid | Delivered::Json | Delivered::Text => {
            IceType::Primitive(PrimitiveType::String)
        }
        Delivered::Bytes => IceType::Primitive(PrimitiveType::Binary),
    }
}

fn build_schema(names: &[String], delivered: &[Delivered]) -> Result<Schema> {
    let fields: Vec<_> = names
        .iter()
        .zip(delivered.iter())
        .enumerate()
        .map(|(i, (n, d))| Arc::new(NestedField::optional(i as i32 + 1, n, ice_type(d))))
        .collect();
    Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .map_err(|e| Error::Transfer(format!("iceberg schema: {e}")))
}

/// Existing table: verify it matches what we deliver and return the table's
/// own field id for each of OUR columns, in our column order — parquet field
/// ids must be the TABLE's ids, whoever created it.
fn conform_ids(meta: &TableMetadata, names: &[String], delivered: &[Delivered]) -> Result<Vec<i32>> {
    let schema = meta.current_schema();
    let theirs = schema.as_struct().fields();
    if theirs.len() != names.len() {
        return Err(Error::InvalidInput(format!(
            "iceberg: destination table has {} columns, the source delivers {} — \
             run once with mode='replace' after dropping the table, or align the schema",
            theirs.len(),
            names.len()
        )));
    }
    let mut ids = Vec::with_capacity(names.len());
    for (n, d) in names.iter().zip(delivered.iter()) {
        let f = theirs.iter().find(|f| &f.name == n).ok_or_else(|| {
            Error::InvalidInput(format!(
                "iceberg: destination table has no column '{n}' — schema drift; \
                 drop the table or align it"
            ))
        })?;
        let want = ice_type(d);
        if f.field_type.as_ref() != &want {
            return Err(Error::InvalidInput(format!(
                "iceberg: column '{n}' is {} in the destination but the source delivers {} — \
                 schema drift; drop the table or align it",
                f.field_type, want
            )));
        }
        ids.push(f.id);
    }
    Ok(ids)
}

/// `s3://bucket/key…` (or s3a) → (bucket, key-without-leading-slash).
fn split_s3_uri(uri: &str) -> Result<(String, String)> {
    let rest = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "iceberg: table location '{uri}' isn't s3:// — only S3-compatible \
                 storage is supported for now"
            ))
        })?;
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    Ok((bucket.to_string(), key.trim_matches('/').to_string()))
}

// ============================================================================
// The sink
// ============================================================================

pub(crate) struct IcebergSink {
    conn: IcebergConn,
    table: String,
    run_id: String,
    // Resolved by dest_state/prepare (both run before loaders):
    meta: Option<TableMetadata>,
    s3: Option<S3Conn>,
    /// Table location, no trailing slash (`s3://bucket/…/table`).
    location: String,
    /// Key prefix inside the bucket for `location` ("" or "…/", '/'-terminated).
    key_prefix: String,
    names: Arc<Vec<String>>,
    delivered: Arc<Vec<Delivered>>,
    field_ids: Arc<Vec<i32>>,
    cursor: Option<(usize, bool)>,
    /// Merge only: (column index, table field id).
    merge_key: Option<(usize, i32)>,
    source_id: Option<String>,
    done: Arc<Mutex<Vec<FileDone>>>,
    next_file: AtomicU64,
}

impl IcebergSink {
    pub(crate) fn bind(conn: IcebergConn, dest_table: &str, _parallel: usize) -> Result<Self> {
        let bare = dest_table.rsplit_once('.').map_or(dest_table, |(_, t)| t);
        Ok(Self {
            conn,
            table: bare.to_string(),
            run_id: uuid::Uuid::new_v4().simple().to_string(),
            meta: None,
            s3: None,
            location: String::new(),
            key_prefix: String::new(),
            names: Arc::new(Vec::new()),
            delivered: Arc::new(Vec::new()),
            field_ids: Arc::new(Vec::new()),
            cursor: None,
            merge_key: None,
            source_id: None,
            done: Arc::new(Mutex::new(Vec::new())),
            next_file: AtomicU64::new(0),
        })
    }

    fn wm_prop(&self) -> String {
        format!(
            "apitap.watermark.{}",
            self.source_id.as_deref().unwrap_or("default")
        )
    }

    /// Bind the S3 side from the catalog-assigned table location.
    fn bind_storage(&mut self, meta: &TableMetadata) -> Result<()> {
        if meta.format_version() == FormatVersion::V1 {
            return Err(Error::InvalidInput(
                "iceberg: this is a format-version 1 table — apitap writes v2 \
                 tables only (v1 is legacy; recreate the table as v2)"
                    .into(),
            ));
        }
        let location = meta.location().trim_end_matches('/').to_string();
        let (bucket, key) = split_s3_uri(&location)?;
        self.key_prefix = if key.is_empty() { String::new() } else { format!("{key}/") };
        self.s3 = Some(S3Conn::from_parts(
            bucket,
            self.conn.s3_endpoint.clone(),
            self.conn.region.clone(),
            self.conn.creds.clone(),
        )?);
        self.location = location;
        self.meta = Some(meta.clone());
        Ok(())
    }

    fn uri_to_key(&self, uri: &str) -> Result<String> {
        split_s3_uri(uri).map(|(_, k)| k)
    }
}

impl IcebergSink {
    /// `max(cursor)` over the destination's CURRENT snapshot, from parquet
    /// footer statistics — the Iceberg equivalent of the Postgres sink's
    /// `SELECT max(cursor)` bootstrap. A few KB of ranged GETs per data file;
    /// no data pages are read.
    async fn dest_data_max(&self, meta: &TableMetadata, cursor: &str) -> Result<Option<String>> {
        use iceberg::spec::{DataContentType as DC, Manifest};
        let s3 = self.s3.clone().expect("storage bound");
        let Some(snap) = meta.current_snapshot() else { return Ok(None) };
        let list_bytes = s3.get_object(&self.uri_to_key(snap.manifest_list())?).await?;
        let list = ManifestList::parse_with_version(&list_bytes, meta.format_version())
            .map_err(|e| Error::Transfer(format!("iceberg: manifest list didn't parse: {e}")))?;
        let mut acc: Option<String> = None;
        let mut numeric = false;
        let mut saw_file = false;
        for mf in list.entries() {
            let m_bytes = s3.get_object(&self.uri_to_key(&mf.manifest_path)?).await?;
            let manifest = Manifest::parse_avro(&m_bytes)
                .map_err(|e| Error::Transfer(format!("iceberg: manifest didn't parse: {e}")))?;
            for entry in manifest.entries() {
                let df = entry.data_file();
                if df.content_type() != DC::Data || !entry.is_alive() {
                    continue;
                }
                saw_file = true;
                let (v, num) = self
                    .footer_cursor_max(&s3, df.file_path(), df.file_size_in_bytes(), cursor)
                    .await?;
                numeric = num;
                acc = wm_max(acc, Some(v), num);
            }
        }
        let _ = numeric;
        if !saw_file {
            return Ok(None);
        }
        Ok(acc)
    }

    /// The cursor column's max in one parquet file, read from its footer.
    async fn footer_cursor_max(
        &self,
        s3: &S3Conn,
        path: &str,
        size: u64,
        cursor: &str,
    ) -> Result<(String, bool)> {
        use parquet::basic::{LogicalType, TimeUnit, Type as PhysicalType};
        use parquet::file::metadata::ParquetMetaDataReader;
        use parquet::file::statistics::Statistics;

        let key = self.uri_to_key(path)?;
        let tail_len = size.min(128 * 1024);
        let mut tail = s3.get_range(&key, size - tail_len, tail_len).await?;
        if tail.len() < 8 || &tail[tail.len() - 4..] != b"PAR1" {
            return Err(Error::Transfer(format!("iceberg: {path} isn't parquet")));
        }
        let flen =
            u32::from_le_bytes(tail[tail.len() - 8..tail.len() - 4].try_into().unwrap()) as u64;
        if flen + 8 > tail_len {
            tail = s3.get_range(&key, size - flen - 8, flen + 8).await?;
        }
        let meta_slice = &tail[tail.len() - 8 - flen as usize..tail.len() - 8];
        let md = ParquetMetaDataReader::decode_metadata(meta_slice)
            .map_err(|e| Error::Transfer(format!("iceberg: {path} footer: {e}")))?;
        let descr = md.file_metadata().schema_descr();
        let idx = (0..descr.num_columns())
            .find(|&i| descr.column(i).name() == cursor)
            .ok_or_else(|| {
                Error::Transfer(format!(
                    "iceberg bootstrap: data file {path} has no column '{cursor}'"
                ))
            })?;
        let col = descr.column(idx);
        let no_stats = || {
            Error::InvalidInput(format!(
                "iceberg: can't derive a bootstrap watermark for '{cursor}' from \
                 {path} (missing/inexact column statistics) — run once with \
                 mode='replace', or use an integer/timestamp cursor"
            ))
        };
        let mut max_i: Option<i64> = None;
        let mut max_s: Option<String> = None;
        for rg in md.row_groups() {
            match rg.column(idx).statistics() {
                Some(Statistics::Int64(v)) => {
                    let m = *v.max_opt().ok_or_else(no_stats)?;
                    max_i = Some(max_i.map_or(m, |a| a.max(m)));
                }
                Some(Statistics::Int32(v)) => {
                    let m = *v.max_opt().ok_or_else(no_stats)? as i64;
                    max_i = Some(max_i.map_or(m, |a| a.max(m)));
                }
                Some(Statistics::ByteArray(v)) => {
                    if !v.max_is_exact() {
                        return Err(no_stats());
                    }
                    let m = v
                        .max_opt()
                        .and_then(|b| std::str::from_utf8(b.data()).ok())
                        .ok_or_else(no_stats)?
                        .to_string();
                    max_s = Some(max_s.map_or(m.clone(), |a| a.max(m)));
                }
                _ => return Err(no_stats()),
            }
        }
        match (col.physical_type(), col.logical_type(), max_i, max_s) {
            (PhysicalType::INT64, Some(LogicalType::Timestamp { is_adjusted_to_u_t_c, unit }), Some(m), _) => {
                if !matches!(unit, TimeUnit::MICROS(_)) {
                    return Err(Error::InvalidInput(format!(
                        "iceberg bootstrap: '{cursor}' uses a non-microsecond \
                         timestamp unit — not supported"
                    )));
                }
                Ok((
                    crate::wire::bqparquet::render_ts_micros(m, is_adjusted_to_u_t_c)?,
                    false,
                ))
            }
            (PhysicalType::INT32, Some(LogicalType::Date), Some(m), _) => {
                Ok((crate::wire::bqparquet::render_date_days(m)?, false))
            }
            (PhysicalType::INT64 | PhysicalType::INT32, None | Some(LogicalType::Integer { .. }), Some(m), _) => {
                Ok((m.to_string(), true))
            }
            (PhysicalType::BYTE_ARRAY, _, _, Some(m)) => Ok((m, false)),
            _ => Err(no_stats()),
        }
    }
}

impl crate::sink::Sink for IcebergSink {
    type Loader = IcebergLoader;

    fn accepts(&self) -> &[WireFormat] {
        &[WireFormat::PgCopyBinary]
    }

    async fn dest_state(
        &mut self,
        plan: &mut TablePlan,
        mode: Mode,
        cursor: &str,
        source_id: &str,
    ) -> Result<DestState> {
        self.source_id = Some(source_id.to_string());
        if mode == Mode::Merge {
            match plan.pk_cols.as_slice() {
                [_single] => {}
                [] => {
                    return Err(Error::InvalidInput(
                        "iceberg merge needs a primary key on the source table".into(),
                    ))
                }
                many => {
                    return Err(Error::InvalidInput(format!(
                        "iceberg merge supports a single-column merge key for now — \
                         the source PK is composite ({})",
                        many.join(", ")
                    )))
                }
            }
        }
        let _ = cursor;
        self.conn.ensure_namespace().await?;
        let Some(meta) = self.conn.load_table(&self.table).await? else {
            return Ok(DestState { exists: false, watermark: None });
        };
        // Name-level drift check now (types conform in prepare, where the
        // lane's delivered types exist): a mis-append must fail before bytes move.
        let theirs = meta.current_schema().as_struct().fields().to_vec();
        for c in &plan.cols {
            if !theirs.iter().any(|f| f.name == c.name) {
                return Err(Error::InvalidInput(format!(
                    "iceberg: destination table has no column '{}' — schema drift; \
                     drop the table or align it",
                    c.name
                )));
            }
        }
        if theirs.len() != plan.cols.len() {
            return Err(Error::InvalidInput(format!(
                "iceberg: destination table has {} columns, the source delivers {} — \
                 schema drift; drop the table or align it",
                theirs.len(),
                plan.cols.len()
            )));
        }
        let props = meta.properties();
        let own = props.get(&self.wm_prop()).cloned();
        let sibling = props
            .keys()
            .any(|k| k.starts_with("apitap.watermark.") && *k != self.wm_prop());
        // No state anywhere (fresh table, or the last write was a replace,
        // which clears state): bootstrap from the DATA, like the Postgres
        // sink's `SELECT max(cursor)` — here that means the parquet footer
        // statistics of the committed data files, a few KB of ranged GETs.
        // Works on tables apitap never wrote (Spark/Trino/pyiceberg output).
        let data_max = if own.is_none() && !sibling {
            self.bind_storage(&meta)?;
            self.dest_data_max(&meta, cursor).await?
        } else {
            None
        };
        // The watermark property lands in the SAME catalog commit as the data
        // — state alone is authoritative, like the Postgres sink.
        let watermark = resolve_watermark(
            own,
            data_max,
            sibling,
            WmArbitration::StateAuthoritative,
            "iceberg",
            source_id,
        )?;
        Ok(DestState { exists: true, watermark })
    }

    async fn prepare(
        &mut self,
        plan: &TablePlan,
        lane: &Lane,
        _durable: bool,
        mode: Mode,
    ) -> Result<()> {
        let mut names = Vec::new();
        let mut delivered = Vec::new();
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
        self.conn.ensure_namespace().await?;
        let meta = match self.conn.load_table(&self.table).await? {
            Some(m) => m,
            None => {
                let schema = build_schema(&names, &delivered)?;
                self.conn.create_table(&self.table, &schema).await?
            }
        };
        let ids = conform_ids(&meta, &names, &delivered)?;
        self.cursor = plan.cursor.as_ref().and_then(|cur| {
            names.iter().position(|n| n == cur).map(|i| {
                (i, matches!(delivered[i], Delivered::Int { .. }))
            })
        });
        if mode == Mode::Merge {
            let key = plan.pk_cols.first().cloned().ok_or_else(|| {
                Error::InvalidInput("iceberg merge needs a primary key".into())
            })?;
            let idx = names.iter().position(|n| n == &key).ok_or_else(|| {
                Error::InvalidInput(format!("iceberg merge: key column '{key}' not delivered"))
            })?;
            self.merge_key = Some((idx, ids[idx]));
        }
        self.bind_storage(&meta)?;
        self.names = Arc::new(names);
        self.delivered = Arc::new(delivered);
        self.field_ids = Arc::new(ids);
        Ok(())
    }

    async fn loader(&self) -> Result<IcebergLoader> {
        let s3 = self.s3.clone().expect("prepare ran");
        let n = self.next_file.fetch_add(1, Ordering::Relaxed);
        let key = format!("{}data/{}-{n:05}.parquet", self.key_prefix, self.run_id);
        let path = format!("{}/data/{}-{n:05}.parquet", self.location, self.run_id);
        let upload_id = s3.create_multipart(&key).await?;
        let pq = ParquetEncoder::new_ext(
            self.names.as_ref().clone(),
            self.delivered.as_ref().clone(),
            self.cursor,
            Some(self.field_ids.as_ref().clone()),
            self.merge_key.map(|(i, _)| i),
        )?;
        Ok(IcebergLoader {
            s3,
            key,
            path,
            upload_id,
            etags: Vec::new(),
            part_no: 0,
            pq,
            rows: 0,
            bytes: 0,
            done: self.done.clone(),
        })
    }

    async fn rows_staged(&self, loaded: u64) -> Result<u64> {
        Ok(loaded)
    }

    async fn finalize(&self, rows: u64, mode: Mode) -> Result<()> {
        let s3 = self.s3.clone().expect("prepare ran");
        let files = std::mem::take(&mut *self.done.lock().expect("done list"));
        if rows == 0 {
            for f in &files {
                let _ = s3.delete(&f.key).await;
            }
            return Ok(());
        }
        match self.commit(&s3, &files, mode).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // The commit never happened — no snapshot references these
                // objects; sweep them so a failed run leaves no stray bytes.
                for f in &files {
                    let _ = s3.delete(&f.key).await;
                }
                Err(e)
            }
        }
    }
}

impl IcebergSink {
    async fn commit(&self, s3: &S3Conn, files: &[FileDone], mode: Mode) -> Result<()> {
        let meta0 = self.meta.clone().expect("prepare ran");
        let schema = meta0.current_schema().clone();
        let pspec = meta0.default_partition_spec().as_ref().clone();

        // -- data files (immutable across commit retries)
        let added_rows: u64 = files.iter().map(|f| f.rows).sum();
        let added_bytes: u64 = files.iter().map(|f| f.bytes).sum();
        let data_files: Vec<DataFile> = files
            .iter()
            .map(|f| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(f.path.clone())
                    .file_format(DataFileFormat::Parquet)
                    .partition(Struct::empty())
                    .record_count(f.rows)
                    .file_size_in_bytes(f.bytes)
                    .build()
                    .map_err(|e| Error::Transfer(format!("iceberg data file: {e}")))
            })
            .collect::<Result<_>>()?;

        // -- merge: one equality-delete file for the delta's keys
        let delete_file: Option<(DataFile, String)> = if mode == Mode::Merge {
            let (idx, fid) = self.merge_key.expect("merge key resolved in prepare");
            let mut ints: Vec<i64> = Vec::new();
            let mut texts: Vec<String> = Vec::new();
            for f in files {
                match &f.keys {
                    KeyCap::Int(v) => ints.extend_from_slice(v),
                    KeyCap::Text(v) => texts.extend_from_slice(&v[..]),
                    KeyCap::None => {}
                }
            }
            let n = ints.len() + texts.len();
            if n as u64 != added_rows {
                return Err(Error::Transfer(format!(
                    "iceberg merge: captured {n} keys for {added_rows} rows"
                )));
            }
            let name = &self.names[idx];
            let d = &self.delivered[idx];
            let bytes = write_delete_parquet(name, d, fid, &ints, &texts)?;
            let key = format!("{}data/{}-deletes.parquet", self.key_prefix, self.run_id);
            let path = format!("{}/data/{}-deletes.parquet", self.location, self.run_id);
            let size = bytes.len() as u64;
            s3.put_object(&key, bytes).await?;
            let df = DataFileBuilder::default()
                .content(DataContentType::EqualityDeletes)
                .file_path(path)
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .record_count(n as u64)
                .file_size_in_bytes(size)
                .equality_ids(Some(vec![fid]))
                .build()
                .map_err(|e| Error::Transfer(format!("iceberg delete file: {e}")))?;
            Some((df, key))
        } else {
            None
        };

        // -- new watermark: the freshest cursor value this run shipped
        let numeric = self.cursor.map(|(_, n)| n).unwrap_or(false);
        let new_wm = files
            .iter()
            .fold(None, |acc, f| wm_max(acc, f.wm.clone(), numeric));

        // -- optimistic-concurrency commit loop
        let mut meta = meta0;
        for attempt in 0..3u32 {
            let io = FileIO::new_with_memory();
            let snapshot_id = {
                // Micros-since-epoch xor'd with fresh randomness, masked
                // positive — unique against the snapshot list by check below.
                let mut id = (uuid::Uuid::new_v4().as_u128() as i64
                    ^ chrono::Utc::now().timestamp_micros())
                    & i64::MAX;
                while meta.snapshots().any(|s| s.snapshot_id() == id) {
                    id = (id + 1) & i64::MAX;
                }
                id
            };
            let seq = meta.next_sequence_number();
            let parent = meta.current_snapshot_id();

            // Data manifest (avro via memory FileIO, PUT by our own client).
            let mut manifests: Vec<ManifestFile> = Vec::new();
            if matches!(mode, Mode::Append | Mode::Merge) {
                if let Some(snap) = meta.current_snapshot() {
                    let list_key = self.uri_to_key(snap.manifest_list())?;
                    let bytes = s3.get_object(&list_key).await?;
                    let list = ManifestList::parse_with_version(&bytes, meta.format_version())
                        .map_err(|e| {
                            Error::Transfer(format!("iceberg: manifest list didn't parse: {e}"))
                        })?;
                    manifests.extend(list.consume_entries());
                }
            }
            let (mut data_mf, data_bytes) = write_manifest_avro(
                &io,
                schema.clone(),
                pspec.clone(),
                snapshot_id,
                false,
                &data_files,
                &format!("memory://{}-m0-a{attempt}.avro", self.run_id),
            )
            .await?;
            let data_key =
                format!("{}metadata/{}-a{attempt}-m0.avro", self.key_prefix, self.run_id);
            s3.put_object(&data_key, data_bytes).await?;
            data_mf.manifest_path = format!(
                "{}/metadata/{}-a{attempt}-m0.avro",
                self.location, self.run_id
            );
            manifests.push(data_mf);

            if let Some((df, _)) = &delete_file {
                let (mut del_mf, del_bytes) = write_manifest_avro(
                    &io,
                    schema.clone(),
                    pspec.clone(),
                    snapshot_id,
                    true,
                    std::slice::from_ref(df),
                    &format!("memory://{}-m1-a{attempt}.avro", self.run_id),
                )
                .await?;
                let del_key =
                    format!("{}metadata/{}-a{attempt}-m1.avro", self.key_prefix, self.run_id);
                s3.put_object(&del_key, del_bytes).await?;
                del_mf.manifest_path = format!(
                    "{}/metadata/{}-a{attempt}-m1.avro",
                    self.location, self.run_id
                );
                manifests.push(del_mf);
            }

            // Manifest list.
            let list_buf = Arc::new(Mutex::new(Vec::new()));
            {
                let mut lw = ManifestListWriter::v2(
                    Box::new(SharedWrite(list_buf.clone())),
                    snapshot_id,
                    parent,
                    seq,
                );
                lw.add_manifests(manifests.into_iter())
                    .map_err(|e| Error::Transfer(format!("iceberg manifest list: {e}")))?;
                lw.close()
                    .await
                    .map_err(|e| Error::Transfer(format!("iceberg manifest list: {e}")))?;
            }
            let list_key = format!(
                "{}metadata/snap-{snapshot_id}-{attempt}-{}.avro",
                self.key_prefix, self.run_id
            );
            let list_path = format!(
                "{}/metadata/snap-{snapshot_id}-{attempt}-{}.avro",
                self.location, self.run_id
            );
            let list_bytes = std::mem::take(&mut *list_buf.lock().expect("list buf"));
            s3.put_object(&list_key, list_bytes).await?;

            // Snapshot + updates + requirements.
            let operation = match mode {
                Mode::Replace => Operation::Overwrite,
                Mode::Append => Operation::Append,
                Mode::Merge => Operation::Overwrite, // row-delta commits stamp overwrite
            };
            let mut props = HashMap::from([
                ("added-data-files".to_string(), data_files.len().to_string()),
                ("added-records".to_string(), added_rows.to_string()),
                ("added-files-size".to_string(), added_bytes.to_string()),
            ]);
            if let Some((df, _)) = &delete_file {
                props.insert("added-delete-files".to_string(), "1".to_string());
                props.insert(
                    "added-equality-deletes".to_string(),
                    df.record_count().to_string(),
                );
            }
            let snapshot = Snapshot::builder()
                .with_manifest_list(list_path)
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(seq)
                .with_summary(Summary { operation, additional_properties: props })
                .with_schema_id(meta.current_schema_id())
                .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
                .build();

            let mut updates = vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ];
            // Watermark state rides the same commit. Replace clears EVERY
            // source's state and writes none (house semantics: a replace run
            // never seeds state; the next incremental bootstraps from data —
            // here, footer statistics). Append/merge write their own key.
            if mode == Mode::Replace {
                let stale: Vec<String> = meta
                    .properties()
                    .keys()
                    .filter(|k| k.starts_with("apitap.watermark."))
                    .cloned()
                    .collect();
                if !stale.is_empty() {
                    updates.push(TableUpdate::RemoveProperties { removals: stale });
                }
            } else if let Some(wm) = &new_wm {
                updates.push(TableUpdate::SetProperties {
                    updates: HashMap::from([(self.wm_prop(), wm.clone())]),
                });
            }
            let requirements = vec![
                TableRequirement::UuidMatch { uuid: meta.uuid() },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: parent,
                },
            ];

            if self
                .conn
                .commit_table(&self.table, &requirements, &updates)
                .await?
            {
                return Ok(());
            }
            // Someone committed between our load and now: reload, re-derive
            // parent/sequence, rebuild manifests, try again.
            meta = self
                .conn
                .load_table(&self.table)
                .await?
                .ok_or_else(|| Error::Transfer("iceberg: table vanished mid-commit".into()))?;
        }
        Err(Error::Transfer(
            "iceberg: commit conflicted 3 times — another writer is racing this \
             table; re-run the transfer"
                .into(),
        ))
    }
}

/// One Avro manifest, written through an in-memory FileIO (the spec writers
/// demand an `OutputFile`), handed back as bytes for our own PUT.
async fn write_manifest_avro(
    io: &FileIO,
    schema: iceberg::spec::SchemaRef,
    pspec: iceberg::spec::PartitionSpec,
    snapshot_id: i64,
    deletes: bool,
    dfs: &[DataFile],
    mem_path: &str,
) -> Result<(ManifestFile, Vec<u8>)> {
    let out = io
        .new_output(mem_path)
        .map_err(|e| Error::Transfer(format!("iceberg manifest io: {e}")))?;
    let b = ManifestWriterBuilder::new(out, Some(snapshot_id), schema, pspec);
    let mut w = if deletes { b.build_v2_deletes() } else { b.build_v2_data() };
    for df in dfs {
        w.add_file(df.clone(), UNASSIGNED_SEQUENCE_NUMBER)
            .map_err(|e| Error::Transfer(format!("iceberg manifest: {e}")))?;
    }
    let mf = w
        .write_manifest_file()
        .await
        .map_err(|e| Error::Transfer(format!("iceberg manifest: {e}")))?;
    let bytes = io
        .new_input(mem_path)
        .map_err(|e| Error::Transfer(format!("iceberg manifest io: {e}")))?
        .read()
        .await
        .map_err(|e| Error::Transfer(format!("iceberg manifest io: {e}")))?
        .to_vec();
    Ok((mf, bytes))
}

/// `FileWrite` into a shared Vec — the manifest-list writer wants an owned
/// `Box<dyn FileWrite>`, we want the bytes back to PUT ourselves.
struct SharedWrite(Arc<Mutex<Vec<u8>>>);

#[async_trait::async_trait]
impl FileWrite for SharedWrite {
    async fn write(&mut self, bs: bytes::Bytes) -> iceberg::Result<()> {
        self.0.lock().expect("shared write").extend_from_slice(&bs);
        Ok(())
    }
    async fn close(&mut self) -> iceberg::Result<()> {
        Ok(())
    }
}

/// A one-column parquet file holding the merge keys (the equality-delete
/// file). Field id must be the TABLE's id for that column.
fn write_delete_parquet(
    name: &str,
    d: &Delivered,
    field_id: i32,
    ints: &[i64],
    texts: &[String],
) -> Result<Vec<u8>> {
    use parquet::basic::{Compression, LogicalType, Repetition, Type as PhysicalType};
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;

    let field = match d {
        Delivered::Int { .. } => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(field_id))
            .build(),
        _ => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::String))
            .with_id(Some(field_id))
            .build(),
    }
    .map_err(|e| Error::Transfer(format!("delete parquet schema: {e}")))?;
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .map_err(|e| Error::Transfer(format!("delete parquet schema: {e}")))?,
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(1).expect("zstd level 1 is always valid"),
            ))
            .build(),
    );
    let mut w = SerializedFileWriter::new(Vec::new(), schema, props)
        .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?;
    {
        let mut rg = w
            .next_row_group()
            .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?;
        let mut col = rg
            .next_column()
            .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?
            .expect("one column");
        let defs: Vec<i16>;
        match d {
            Delivered::Int { .. } => {
                defs = vec![1; ints.len()];
                col.typed::<Int64Type>()
                    .write_batch(ints, Some(&defs), None)
                    .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?;
            }
            _ => {
                let vals: Vec<ByteArray> =
                    texts.iter().map(|s| ByteArray::from(s.as_str())).collect();
                defs = vec![1; vals.len()];
                col.typed::<ByteArrayType>()
                    .write_batch(&vals, Some(&defs), None)
                    .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?;
            }
        }
        col.close()
            .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?;
        rg.close()
            .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))?;
    }
    w.into_inner()
        .map_err(|e| Error::Transfer(format!("delete parquet: {e}")))
}

// ============================================================================
// Loader — one parquet data file per worker, multipart-streamed
// ============================================================================

pub(crate) struct IcebergLoader {
    s3: S3Conn,
    key: String,
    path: String,
    upload_id: String,
    etags: Vec<(u32, String)>,
    part_no: u32,
    pq: ParquetEncoder,
    rows: u64,
    bytes: u64,
    done: Arc<Mutex<Vec<FileDone>>>,
}

impl IcebergLoader {
    async fn flush_part(&mut self, bytes: Vec<u8>) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.part_no += 1;
        self.bytes += bytes.len() as u64;
        let etag = self
            .s3
            .upload_part(&self.key, &self.upload_id, self.part_no, bytes)
            .await?;
        self.etags.push((self.part_no, etag));
        Ok(())
    }
}

impl crate::sink::Loader for IcebergLoader {
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
            self.s3.abort_multipart(&self.key, &self.upload_id).await;
            return Ok(0);
        }
        self.flush_part(pending).await?;
        self.s3
            .complete_multipart(&self.key, &self.upload_id, &self.etags)
            .await?;
        self.done.lock().expect("done list").push(FileDone {
            path: self.path,
            key: self.key,
            rows: self.rows,
            bytes: self.bytes,
            wm: self.pq.wm.take(),
            keys: std::mem::replace(&mut self.pq.keys, KeyCap::None),
        });
        Ok(self.rows)
    }

    async fn abort(self, cause: Error) -> Error {
        self.s3.abort_multipart(&self.key, &self.upload_id).await;
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_uri_split_handles_shapes() {
        assert_eq!(
            split_s3_uri("s3://lake/wh/ns/t").unwrap(),
            ("lake".into(), "wh/ns/t".into())
        );
        assert_eq!(split_s3_uri("s3a://lake").unwrap(), ("lake".into(), "".into()));
        assert!(split_s3_uri("gs://nope").is_err());
    }

    #[test]
    fn delivered_types_map_to_iceberg() {
        assert_eq!(
            ice_type(&Delivered::Int { bytes: 8, unsigned: false }),
            IceType::Primitive(PrimitiveType::Long)
        );
        assert_eq!(
            ice_type(&Delivered::DateTime { utc: true }),
            IceType::Primitive(PrimitiveType::Timestamptz)
        );
        assert_eq!(
            ice_type(&Delivered::Decimal { p: 0, s: 0 }),
            IceType::Primitive(PrimitiveType::Decimal { precision: 38, scale: 0 })
        );
        assert_eq!(ice_type(&Delivered::Uuid), IceType::Primitive(PrimitiveType::String));
    }

    #[test]
    fn delete_parquet_is_a_readable_file() {
        let bytes = write_delete_parquet(
            "id",
            &Delivered::Int { bytes: 8, unsigned: false },
            1,
            &[1, 2, 3],
            &[],
        )
        .unwrap();
        // PAR1 magic at both ends — a structurally complete file.
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }
}
