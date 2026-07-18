//! GitHub PROJECT-DATA source: `github+api://owner/repo`.
//!
//! Where `github://` reads a repo's FILES, this source reads the project
//! itself — issues, pull requests, commits, stars, releases … — as TYPED
//! tables. Every entity ships a curated column set (the fields analysts
//! actually query: numbers as int8, flags as bool, times as timestamptz)
//! plus one `raw` jsonb column carrying the WHOLE API object, so nothing the
//! API returned is ever lost. Deterministic, declared schemas — no inference.
//!
//! Incremental: entities whose API supports `?since=` (issues, issue_comments,
//! commits) work with `mode="append"`/`"merge"` on their update cursor — the
//! watermark lives in `_apitap_state` at the destination like every other
//! route. Entities without a server-side filter refuse incremental loudly.
//!
//! Paging is the REST list protocol (100/page, Link header). `GITHUB_TOKEN`
//! (or `GH_TOKEN`) lifts the API rate limit 60/h → 5,000/h and serves private
//! repos; the token rides only in the Authorization header.

use crate::error::{Error, Result};
use crate::plan::{ColumnPlan, Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::sink::Loader;
use crate::source::Source;
use crate::wire::mytsv::tsv_escape;
use crate::wire::pgcopy as pgc;
use crate::wire::rowbinary::{rb_type, Transcoder};
use serde_json::Value;

const API: &str = "https://api.github.com";
const PER_PAGE: u32 = 100;

/* ------------------------------------------------------------------ entity spec */

/// How one column's value is pulled out of the API object.
#[derive(Clone, Copy)]
enum Ex {
    /// Integer at this JSON path.
    I64(&'static [&'static str]),
    /// String at this JSON path.
    Txt(&'static [&'static str]),
    /// RFC-3339 timestamp at this JSON path.
    Ts(&'static [&'static str]),
    /// Boolean at this JSON path.
    Bool(&'static [&'static str]),
    /// A JSON subtree (arrays like labels) — lands as jsonb text.
    Json(&'static [&'static str]),
    /// The whole API object, verbatim.
    Raw,
    /// Trailing integer of a URL string (issue_comments → issue number).
    TailNum(&'static [&'static str]),
}

struct Col {
    name: &'static str,
    udt: &'static str,
    ex: Ex,
    pk: bool,
}

const fn c(name: &'static str, udt: &'static str, ex: Ex) -> Col {
    Col { name, udt, ex, pk: false }
}
const fn pk(name: &'static str, udt: &'static str, ex: Ex) -> Col {
    Col { name, udt, ex, pk: true }
}

struct Entity {
    name: &'static str,
    /// Path under /repos/{owner}/{repo}/.
    path: &'static str,
    /// Extra fixed query params.
    query: &'static [(&'static str, &'static str)],
    /// Response is `{ wrap: [...] }` instead of a bare array (workflow_runs).
    wrap: Option<&'static str>,
    /// Non-default Accept header (stargazers need star+json for starred_at).
    accept: Option<&'static str>,
    /// The API supports `?since=` server-side → incremental works.
    since: bool,
    /// Cursor column for incremental (documented; passed via cursor=).
    cursor: Option<&'static str>,
    /// The /issues endpoint interleaves PRs — drop objects carrying this key.
    drop_if_key: Option<&'static str>,
    cols: &'static [Col],
}

const ENTITIES: &[Entity] = &[
    Entity {
        name: "issues",
        path: "issues",
        query: &[("state", "all"), ("sort", "updated"), ("direction", "asc")],
        wrap: None,
        accept: None,
        since: true,
        cursor: Some("updated_at"),
        drop_if_key: Some("pull_request"),
        cols: &[
            pk("id", "int8", Ex::I64(&["id"])),
            c("number", "int8", Ex::I64(&["number"])),
            c("title", "text", Ex::Txt(&["title"])),
            c("state", "text", Ex::Txt(&["state"])),
            c("author", "text", Ex::Txt(&["user", "login"])),
            c("labels", "jsonb", Ex::Json(&["labels"])),
            c("comments", "int8", Ex::I64(&["comments"])),
            c("created_at", "timestamptz", Ex::Ts(&["created_at"])),
            c("updated_at", "timestamptz", Ex::Ts(&["updated_at"])),
            c("closed_at", "timestamptz", Ex::Ts(&["closed_at"])),
            c("body", "text", Ex::Txt(&["body"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "pull_requests",
        path: "pulls",
        query: &[("state", "all"), ("sort", "updated"), ("direction", "asc")],
        wrap: None,
        accept: None,
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("id", "int8", Ex::I64(&["id"])),
            c("number", "int8", Ex::I64(&["number"])),
            c("title", "text", Ex::Txt(&["title"])),
            c("state", "text", Ex::Txt(&["state"])),
            c("author", "text", Ex::Txt(&["user", "login"])),
            c("base_ref", "text", Ex::Txt(&["base", "ref"])),
            c("head_ref", "text", Ex::Txt(&["head", "ref"])),
            c("draft", "bool", Ex::Bool(&["draft"])),
            c("created_at", "timestamptz", Ex::Ts(&["created_at"])),
            c("updated_at", "timestamptz", Ex::Ts(&["updated_at"])),
            c("merged_at", "timestamptz", Ex::Ts(&["merged_at"])),
            c("closed_at", "timestamptz", Ex::Ts(&["closed_at"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "commits",
        path: "commits",
        query: &[],
        wrap: None,
        accept: None,
        since: true,
        cursor: Some("committed_at"),
        drop_if_key: None,
        cols: &[
            pk("sha", "text", Ex::Txt(&["sha"])),
            c("author_name", "text", Ex::Txt(&["commit", "author", "name"])),
            c("author_login", "text", Ex::Txt(&["author", "login"])),
            c("message", "text", Ex::Txt(&["commit", "message"])),
            c("authored_at", "timestamptz", Ex::Ts(&["commit", "author", "date"])),
            c("committed_at", "timestamptz", Ex::Ts(&["commit", "committer", "date"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "stargazers",
        path: "stargazers",
        query: &[],
        wrap: None,
        accept: Some("application/vnd.github.star+json"),
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("user_id", "int8", Ex::I64(&["user", "id"])),
            c("user_login", "text", Ex::Txt(&["user", "login"])),
            c("starred_at", "timestamptz", Ex::Ts(&["starred_at"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "releases",
        path: "releases",
        query: &[],
        wrap: None,
        accept: None,
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("id", "int8", Ex::I64(&["id"])),
            c("tag_name", "text", Ex::Txt(&["tag_name"])),
            c("name", "text", Ex::Txt(&["name"])),
            c("author", "text", Ex::Txt(&["author", "login"])),
            c("draft", "bool", Ex::Bool(&["draft"])),
            c("prerelease", "bool", Ex::Bool(&["prerelease"])),
            c("created_at", "timestamptz", Ex::Ts(&["created_at"])),
            c("published_at", "timestamptz", Ex::Ts(&["published_at"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "issue_comments",
        path: "issues/comments",
        query: &[("sort", "updated"), ("direction", "asc")],
        wrap: None,
        accept: None,
        since: true,
        cursor: Some("updated_at"),
        drop_if_key: None,
        cols: &[
            pk("id", "int8", Ex::I64(&["id"])),
            c("issue_number", "int8", Ex::TailNum(&["issue_url"])),
            c("issue_url", "text", Ex::Txt(&["issue_url"])),
            c("author", "text", Ex::Txt(&["user", "login"])),
            c("created_at", "timestamptz", Ex::Ts(&["created_at"])),
            c("updated_at", "timestamptz", Ex::Ts(&["updated_at"])),
            c("body", "text", Ex::Txt(&["body"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "workflow_runs",
        path: "actions/runs",
        query: &[],
        wrap: Some("workflow_runs"),
        accept: None,
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("id", "int8", Ex::I64(&["id"])),
            c("name", "text", Ex::Txt(&["name"])),
            c("event", "text", Ex::Txt(&["event"])),
            c("status", "text", Ex::Txt(&["status"])),
            c("conclusion", "text", Ex::Txt(&["conclusion"])),
            c("branch", "text", Ex::Txt(&["head_branch"])),
            c("sha", "text", Ex::Txt(&["head_sha"])),
            c("created_at", "timestamptz", Ex::Ts(&["created_at"])),
            c("updated_at", "timestamptz", Ex::Ts(&["updated_at"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "branches",
        path: "branches",
        query: &[],
        wrap: None,
        accept: None,
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("name", "text", Ex::Txt(&["name"])),
            c("sha", "text", Ex::Txt(&["commit", "sha"])),
            c("protected", "bool", Ex::Bool(&["protected"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "tags",
        path: "tags",
        query: &[],
        wrap: None,
        accept: None,
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("name", "text", Ex::Txt(&["name"])),
            c("sha", "text", Ex::Txt(&["commit", "sha"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
    Entity {
        name: "labels",
        path: "labels",
        query: &[],
        wrap: None,
        accept: None,
        since: false,
        cursor: None,
        drop_if_key: None,
        cols: &[
            pk("id", "int8", Ex::I64(&["id"])),
            c("name", "text", Ex::Txt(&["name"])),
            c("color", "text", Ex::Txt(&["color"])),
            c("description", "text", Ex::Txt(&["description"])),
            c("is_default", "bool", Ex::Bool(&["default"])),
            c("raw", "jsonb", Ex::Raw),
        ],
    },
];

fn entity(name: &str) -> Result<&'static Entity> {
    ENTITIES.iter().find(|e| e.name == name).ok_or_else(|| {
        Error::InvalidInput(format!(
            "unknown github+api table '{name}' (tables: {})",
            ENTITIES.iter().map(|e| e.name).collect::<Vec<_>>().join(", ")
        ))
    })
}

/* ------------------------------------------------------------------ values */

fn walk<'a>(v: &'a Value, path: &[&str]) -> &'a Value {
    let mut cur = v;
    for p in path {
        cur = &cur[*p];
    }
    cur
}

/// RFC-3339 → microseconds since the Unix epoch.
fn ts_micros(s: &str) -> Result<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|t| t.timestamp_micros())
        .map_err(|e| Error::Transfer(format!("github+api timestamp '{s}': {e}")))
}

/// The destination's watermark (a SQL literal — possibly quoted, PG
/// `2026-01-01 00:00:00+00` or CH `2026-01-01 00:00:00` flavored) → RFC-3339
/// for the `?since=` parameter, plus microseconds for boundary filtering.
fn watermark_parts(literal: &str) -> Result<(String, i64)> {
    let s = literal.trim().trim_matches('\'').trim();
    let mut iso = s.replace(' ', "T");
    if iso.ends_with("+00") {
        iso = format!("{}Z", &iso[..iso.len() - 3]);
    } else if !(iso.ends_with('Z') || iso.contains('+')) {
        // No offset at all (ClickHouse DateTime text) — the engine stores UTC.
        iso.push('Z');
    }
    let micros = ts_micros(&iso)?;
    // Re-render CANONICAL UTC for the query string: an offset like +07:00
    // would leak a literal '+' into the URL (decoded as a space server-side).
    let canonical = chrono::DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| Error::Transfer(format!("watermark out of range: {literal}")))?
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    Ok((canonical, micros))
}

/* ------------------------------------------------------------------ source */

pub(crate) struct GithubApiSource {
    client: reqwest::Client,
    owner: String,
    repo: String,
    token: Option<String>,
    /// Repo metadata (star/issue counts) — one fetch per run, for LPT estimates.
    meta: tokio::sync::OnceCell<Value>,
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

impl GithubApiSource {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let u = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidInput(format!("github+api url: {e}")))?;
        let owner = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "github+api url needs owner and repo: github+api://<owner>/<repo>".into(),
                )
            })?
            .to_string();
        let repo = u.path().trim_matches('/').to_string();
        if repo.is_empty() || repo.contains('/') {
            return Err(Error::InvalidInput(
                "github+api url needs exactly owner/repo: github+api://<owner>/<repo>".into(),
            ));
        }
        if let Some((k, _)) = u.query_pairs().next() {
            return Err(Error::InvalidInput(format!(
                "unknown github+api url parameter '{k}' (auth goes in the GITHUB_TOKEN \
                 env var, never the url)"
            )));
        }
        Ok(Self {
            client: reqwest::Client::new(),
            owner,
            repo,
            token: env_nonempty("GITHUB_TOKEN").or_else(|| env_nonempty("GH_TOKEN")),
            meta: tokio::sync::OnceCell::new(),
        })
    }

    async fn send(&self, url: String, accept: Option<&str>, what: &str) -> Result<reqwest::Response> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let mut r = self
                .client
                .get(&url)
                .header("user-agent", "apitap")
                .header("accept", accept.unwrap_or("application/vnd.github+json"));
            if let Some(t) = &self.token {
                r = r.bearer_auth(t);
            }
            match r.send().await {
                Ok(resp) if resp.status().is_server_error() && attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                }
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(resp);
                    }
                    // Secondary rate limits clear in well under a minute and
                    // GitHub says clients MUST honor Retry-After — wait when the
                    // wait is short instead of aborting a half-streamed table.
                    if matches!(status.as_u16(), 403 | 429) && attempt < 3 {
                        let wait = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .or_else(|| {
                                resp.headers()
                                    .get("x-ratelimit-reset")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|v| v.parse::<u64>().ok())
                                    .map(|reset| {
                                        reset.saturating_sub(
                                            std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .map(|d| d.as_secs())
                                                .unwrap_or(0),
                                        )
                                    })
                            });
                        if let Some(w) = wait {
                            if w <= 120 {
                                tokio::time::sleep(std::time::Duration::from_secs(w.max(1)))
                                    .await;
                                continue;
                            }
                        }
                    }
                    let hint = match (status.as_u16(), self.token.is_some()) {
                        (401, false) => {
                            " (this endpoint requires auth — set GITHUB_TOKEN; \
                             stargazers' starred_at media type is auth-only)"
                        }
                        (404, false) => " (a private repo needs GITHUB_TOKEN)",
                        (403 | 429, false) => {
                            " (rate limited? set GITHUB_TOKEN to lift 60/h → 5,000/h)"
                        }
                        (403 | 429, true) => " (rate limited — authenticated limits reset hourly)",
                        _ => "",
                    };
                    let body = resp.text().await.unwrap_or_default();
                    return Err(Error::Transfer(format!(
                        "github+api {what} ({status}){hint}: {}",
                        body.chars().take(300).collect::<String>().trim()
                    )));
                }
                Err(_) if attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                }
                Err(e) => return Err(Error::Transfer(format!("github+api {what}: {e}"))),
            }
        }
    }

    async fn meta(&self) -> Result<&Value> {
        self.meta
            .get_or_try_init(|| async {
                self.send(
                    format!("{API}/repos/{}/{}", self.owner, self.repo),
                    None,
                    "repo lookup",
                )
                .await?
                .json()
                .await
                .map_err(|e| Error::Transfer(format!("github+api repo response: {e}")))
            })
            .await
    }

    fn first_page_url(&self, e: &Entity, since: Option<&str>) -> String {
        let mut url = format!(
            "{API}/repos/{}/{}/{}?per_page={PER_PAGE}",
            self.owner, self.repo, e.path
        );
        for (k, v) in e.query {
            url.push_str(&format!("&{k}={v}"));
        }
        if let Some(s) = since {
            url.push_str(&format!("&since={s}"));
        }
        url
    }
}

/// The `rel="next"` URL out of a Link header. GitHub's large datasets REQUIRE
/// this (the URL carries an `after=` cursor; a manufactured `page=11` answers
/// HTTP 422) — and cursor pagination also anchors the crawl position, so items
/// updated mid-crawl can't shift rows across page boundaries.
fn link_next(link: &str) -> Option<String> {
    link.split(',').find_map(|part| {
        let (url, rest) = part.split_once(';')?;
        if !rest.contains("rel=\"next\"") {
            return None;
        }
        Some(url.trim().trim_start_matches('<').trim_end_matches('>').to_string())
    })
}

/// Postgres can't store U+0000 in text/jsonb (binary COPY aborts the whole
/// load) — strip it; GitHub bodies are user-controlled bytes.
fn strip_nul(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains('\u{0}') {
        std::borrow::Cow::Owned(s.replace('\u{0}', ""))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

fn strip_json_nul(s: String) -> String {
    // serde_json renders U+0000 as the escape \u0000 — remove it before the
    // jsonb field for the same Postgres reason.
    if s.contains("\\u0000") {
        s.replace("\\u0000", "")
    } else {
        s
    }
}

/// Render micros-since-Unix-epoch as MySQL DATETIME(6) text (sessions run UTC).
fn mysql_datetime(micros: i64) -> Result<String> {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .ok_or_else(|| Error::Transfer(format!("timestamp out of range: {micros}")))
}

/// Encode one API object as a MySQL LOAD DATA text row (\t fields, \N NULLs,
/// [`crate::wire::mytsv`] escapes). An empty string stays '' — distinct from
/// NULL, unlike the all-text sources.
fn encode_row_tsv(e: &Entity, item: &Value, out: &mut Vec<u8>) -> Result<()> {
    let mut itoa = itoa::Buffer::new();
    for (i, col) in e.cols.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match col.ex {
            Ex::Raw => tsv_escape(strip_json_nul(item.to_string()).as_bytes(), out),
            Ex::I64(p) => match walk(item, p).as_i64() {
                Some(v) => out.extend_from_slice(itoa.format(v).as_bytes()),
                None => out.extend_from_slice(b"\\N"),
            },
            Ex::Txt(p) => match walk(item, p).as_str() {
                Some(v) => tsv_escape(v.as_bytes(), out),
                None => out.extend_from_slice(b"\\N"),
            },
            Ex::Bool(p) => match walk(item, p).as_bool() {
                Some(v) => out.push(if v { b'1' } else { b'0' }),
                None => out.extend_from_slice(b"\\N"),
            },
            Ex::Ts(p) => match walk(item, p).as_str() {
                Some(v) => out.extend_from_slice(mysql_datetime(ts_micros(v)?)?.as_bytes()),
                None => out.extend_from_slice(b"\\N"),
            },
            Ex::Json(p) => {
                let v = walk(item, p);
                if v.is_null() {
                    out.extend_from_slice(b"\\N");
                } else {
                    tsv_escape(strip_json_nul(v.to_string()).as_bytes(), out);
                }
            }
            Ex::TailNum(p) => {
                match walk(item, p)
                    .as_str()
                    .and_then(|u| u.rsplit('/').next())
                    .and_then(|t| t.parse::<i64>().ok())
                {
                    Some(v) => out.extend_from_slice(itoa.format(v).as_bytes()),
                    None => out.extend_from_slice(b"\\N"),
                }
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

/// Encode one API object as a PgCopyBinary tuple.
fn encode_row(e: &Entity, item: &Value, out: &mut Vec<u8>) -> Result<()> {
    pgc::tuple_start(e.cols.len(), out);
    for col in e.cols {
        match col.ex {
            Ex::Raw => pgc::jsonb_field(strip_json_nul(item.to_string()).as_bytes(), out),
            Ex::I64(p) => match walk(item, p).as_i64() {
                Some(v) => pgc::field(&v.to_be_bytes(), out),
                None => pgc::null_field(out),
            },
            Ex::Txt(p) => match walk(item, p).as_str() {
                Some(v) => pgc::field(strip_nul(v).as_bytes(), out),
                None => pgc::null_field(out),
            },
            Ex::Bool(p) => match walk(item, p).as_bool() {
                Some(v) => pgc::field(&[v as u8], out),
                None => pgc::null_field(out),
            },
            Ex::Ts(p) => match walk(item, p).as_str() {
                Some(v) => pgc::field(&(ts_micros(v)? - pgc::PG_EPOCH_MICROS).to_be_bytes(), out),
                None => pgc::null_field(out),
            },
            Ex::TailNum(p) => {
                let n = walk(item, p)
                    .as_str()
                    .and_then(|u| u.rsplit('/').next())
                    .and_then(|t| t.parse::<i64>().ok());
                match n {
                    Some(v) => pgc::field(&v.to_be_bytes(), out),
                    None => pgc::null_field(out),
                }
            }
            Ex::Json(p) => {
                let v = walk(item, p);
                if v.is_null() {
                    pgc::null_field(out);
                } else {
                    pgc::jsonb_field(strip_json_nul(v.to_string()).as_bytes(), out);
                }
            }
        }
    }
    Ok(())
}

impl Source for GithubApiSource {
    fn cursor_quoted(&self, udt: &str) -> Result<bool> {
        match udt {
            "timestamptz" => Ok(true),
            "int8" => Ok(false),
            other => Err(Error::InvalidInput(format!(
                "github+api: '{other}' can't be an incremental cursor — use the \
                 entity's update timestamp (issues/issue_comments: updated_at, \
                 commits: committed_at)"
            ))),
        }
    }

    async fn probe(&self, table: &str) -> Result<TablePlan> {
        let e = entity(table)?;
        Ok(TablePlan {
            engine: "github+api",
            cols: e
                .cols
                .iter()
                .map(|col| ColumnPlan {
                    name: col.name.to_string(),
                    nullable: true,
                    int_pk: false,
                    native_ddl: None,
                    udt: col.udt.into(),
                    precision: None,
                    scale: None,
                })
                .collect(),
            cursor: None,
            pk_cols: e
                .cols
                .iter()
                .filter(|c| c.pk)
                .map(|c| c.name.to_string())
                .collect(),
        })
    }

    async fn catalog(
        &self,
        _schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        let meta = self.meta().await?;
        let est = |name: &str| -> i64 {
            match name {
                "issues" => meta["open_issues_count"].as_i64().unwrap_or(-1),
                "stargazers" => meta["stargazers_count"].as_i64().unwrap_or(-1),
                _ => -1,
            }
        };
        match tables {
            Some(ts) => ts
                .iter()
                .map(|t| entity(t).map(|e| (e.name.to_string(), est(e.name))))
                .collect(),
            None => Ok(ENTITIES
                .iter()
                .map(|e| (e.name.to_string(), est(e.name)))
                .collect()),
        }
    }

    fn can_produce(&self, _plan: &TablePlan, format: WireFormat) -> bool {
        // Typed delivery: native PgCopyBinary, RowBinary via the same
        // transcoder the Postgres source uses, and the MySQL text dialect for
        // LOAD DATA (values rendered as text; the Postgres text dialect is
        // never produced).
        matches!(
            format,
            WireFormat::PgCopyBinary | WireFormat::RowBinary | WireFormat::MyTsv
        )
    }

    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane {
        Lane {
            format,
            cols: plan
                .cols
                .iter()
                .map(|c| LaneCol {
                    delivered: match c.udt.as_str() {
                        "int8" => Delivered::Int { bytes: 8, unsigned: false },
                        "bool" => Delivered::Bool,
                        "timestamptz" => Delivered::DateTime { utc: true },
                        "jsonb" => Delivered::Json,
                        _ => Delivered::Text,
                    },
                    select: c.name.clone(),
                })
                .collect(),
        }
    }

    async fn span_stmts(
        &self,
        table: &str,
        _plan: &TablePlan,
        _lane: &Lane,
        _want: usize,
        delta: Option<&Delta>,
    ) -> Result<Vec<String>> {
        let e = entity(table)?;
        let stmt = match delta {
            None => serde_json::json!({ "entity": e.name }),
            Some(d) => {
                if !e.since {
                    return Err(Error::InvalidInput(format!(
                        "github+api '{}' can't sync incrementally — its API has no \
                         server-side since filter; use mode='replace' (incremental \
                         entities: issues, issue_comments, commits)",
                        e.name
                    )));
                }
                // The API's since= filters on the ENTITY'S update clock — a
                // different cursor would filter one column and watermark
                // another, silently wrong. Enforce the pairing.
                if e.cursor != Some(d.col.as_str()) {
                    return Err(Error::InvalidInput(format!(
                        "github+api '{}' syncs incrementally on {} — pass cursor=\"{}\"",
                        e.name,
                        e.cursor.unwrap_or("?"),
                        e.cursor.unwrap_or("?"),
                    )));
                }
                let (iso, micros) = watermark_parts(&d.literal)?;
                serde_json::json!({
                    "entity": e.name,
                    "since": iso,
                    "wm_micros": micros,
                    "cursor": d.col,
                    "strict": d.op == ">",
                })
            }
        };
        Ok(vec![stmt.to_string()])
    }

    async fn run_workers<L: Loader>(
        &self,
        plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        loaders: Vec<L>,
        chunk: usize,
    ) -> Result<u64> {
        let mut loader = loaders
            .into_iter()
            .next()
            .expect("one span always yields one loader");
        let stmt: Value = serde_json::from_str(&stmts[0])
            .map_err(|e| Error::Transfer(format!("github+api span stmt: {e}")))?;
        let e = entity(stmt["entity"].as_str().unwrap_or_default())?;
        let since = stmt["since"].as_str().map(str::to_string);
        let wm_micros = stmt["wm_micros"].as_i64();
        let strict = stmt["strict"].as_bool().unwrap_or(true);
        let cursor_col = stmt["cursor"].as_str().map(str::to_string);
        let cursor_path: Option<&'static [&'static str]> = cursor_col.as_deref().and_then(|cc| {
            e.cols.iter().find(|c| c.name == cc).and_then(|c| match c.ex {
                Ex::Ts(p) => Some(p),
                _ => None,
            })
        });

        // RowBinary lane: run the pgcopy stream through the same transcoder the
        // Postgres source uses — one encoder, every destination.
        let tsv_lane = lane.format == WireFormat::MyTsv;
        let mut xcode = match lane.format {
            WireFormat::PgCopyBinary | WireFormat::MyTsv => None,
            WireFormat::RowBinary => {
                // Nullability comes from the ADJUSTED plan — the ClickHouse
                // sink flips the cursor/order-by column non-Nullable and its
                // DDL and this wire framing must agree (the flag byte would
                // frame-shift a non-Nullable column). Same contract as the
                // Postgres source.
                let cols = plan
                    .cols
                    .iter()
                    .map(|c| {
                        rb_type(&c.udt, None, None)
                            .map(|t| (t, c.nullable))
                            .ok_or_else(|| {
                                Error::Transfer(format!(
                                    "github+api: no RowBinary type for {}",
                                    c.udt
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Some(Transcoder::new(cols))
            }
            WireFormat::TabSeparated => {
                return Err(loader
                    .abort(Error::Transfer(
                        "github+api never negotiates the PG text dialect".into(),
                    ))
                    .await)
            }
        };

        let mut pg: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
        if !tsv_lane {
            pgc::header(&mut pg);
        }
        let mut out: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
        let mut rows: u64 = 0;
        let mut url = self.first_page_url(e, since.as_deref());
        loop {
            let resp = match self.send(url.clone(), e.accept, e.name).await {
                Ok(r) => r,
                Err(err) => return Err(loader.abort(err).await),
            };
            let next = resp
                .headers()
                .get("link")
                .and_then(|v| v.to_str().ok())
                .and_then(link_next);
            let body: Value = match resp.json().await {
                Ok(v) => v,
                Err(err) => {
                    return Err(loader
                        .abort(Error::Transfer(format!("github+api {}: {err}", e.name)))
                        .await)
                }
            };
            let items = match e.wrap {
                Some(k) => body[k].as_array().cloned().unwrap_or_default(),
                None => body.as_array().cloned().unwrap_or_default(),
            };
            for item in &items {
                if let Some(k) = e.drop_if_key {
                    if !item[k].is_null() {
                        continue;
                    }
                }
                // Boundary discipline: `since=` is inclusive server-side; append
                // (op `>`) must not re-land the watermark row.
                if let (Some(wm), Some(p)) = (wm_micros, cursor_path) {
                    if let Some(v) = walk(item, p).as_str() {
                        let m = match ts_micros(v) {
                            Ok(m) => m,
                            Err(err) => return Err(loader.abort(err).await),
                        };
                        if m < wm || (strict && m == wm) {
                            continue;
                        }
                    }
                }
                let enc_res = if tsv_lane {
                    encode_row_tsv(e, item, &mut pg)
                } else {
                    encode_row(e, item, &mut pg)
                };
                if let Err(err) = enc_res {
                    return Err(loader.abort(err).await);
                }
                rows += 1;
                if pg.len() >= chunk {
                    let buf = std::mem::replace(&mut pg, Vec::with_capacity(chunk + 64 * 1024));
                    match &mut xcode {
                        None => loader.send(buf).await?,
                        Some(t) => {
                            if let Err(err) = t.push(&buf, &mut out) {
                                return Err(loader.abort(err).await);
                            }
                            let ready =
                                std::mem::replace(&mut out, Vec::with_capacity(chunk + 64 * 1024));
                            if !ready.is_empty() {
                                loader.send(ready).await?;
                            }
                        }
                    }
                }
            }
            match next {
                Some(n) if !items.is_empty() => url = n,
                _ => break,
            }
        }
        if !tsv_lane {
            pgc::trailer(&mut pg);
        }
        match &mut xcode {
            None => {
                if !pg.is_empty() {
                    loader.send(pg).await?;
                }
            }
            Some(t) => {
                if let Err(err) = t.push(&pg, &mut out) {
                    return Err(loader.abort(err).await);
                }
                if !out.is_empty() {
                    loader.send(out).await?;
                }
            }
        }
        let reported = loader.finish().await?;
        Ok(if reported > 0 { reported } else { rows })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_parse_and_watermarks_convert() {
        assert_eq!(ts_micros("1970-01-01T00:00:01Z").unwrap(), 1_000_000);
        // PG literal (quoted, space, +00) and CH literal (bare) both convert.
        let (iso, m) = watermark_parts("'2026-07-18 10:00:00+00'").unwrap();
        assert_eq!(iso, "2026-07-18T10:00:00Z");
        let (iso2, m2) = watermark_parts("2026-07-18 10:00:00").unwrap();
        assert_eq!(iso2, "2026-07-18T10:00:00Z");
        assert_eq!(m, m2);
    }

    #[test]
    fn issues_row_encodes_and_prs_are_dropped() {
        let e = entity("issues").unwrap();
        let item: Value = serde_json::from_str(
            r#"{"id": 7, "number": 1, "title": "t", "state": "open",
                "user": {"login": "abdul"}, "labels": [{"name": "bug"}],
                "comments": 2, "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z", "closed_at": null,
                "body": "hi"}"#,
        )
        .unwrap();
        let mut out = Vec::new();
        encode_row(e, &item, &mut out).unwrap();
        // 12 columns: 2-byte count then per-field frames; spot the count.
        assert_eq!(&out[..2], &(12i16).to_be_bytes());
        // A PR-flavored object carries "pull_request" — the caller drops it.
        assert_eq!(e.drop_if_key, Some("pull_request"));
    }

    #[test]
    fn link_header_next_is_followed_verbatim() {
        let l = "<https://api.github.com/repositories/1/issues?after=Y3Vyc29y&per_page=100>; rel=\"next\", <https://x>; rel=\"last\"";
        assert_eq!(
            link_next(l).as_deref(),
            Some("https://api.github.com/repositories/1/issues?after=Y3Vyc29y&per_page=100")
        );
        assert_eq!(link_next("<https://x>; rel=\"prev\""), None);
    }

    #[test]
    fn nul_bytes_never_reach_postgres() {
        assert_eq!(strip_nul("a\u{0}b"), "ab");
        assert_eq!(strip_nul("clean"), "clean");
        let v: Value = serde_json::json!({"t": "a\u{0}b"});
        assert!(!strip_json_nul(v.to_string()).contains("\\u0000"));
    }

    #[test]
    fn watermark_since_is_always_utc_z() {
        // A +07:00 literal must not leak '+' into the query string.
        let (iso, _) = watermark_parts("'2026-07-18 10:00:00+07:00'").unwrap();
        assert_eq!(iso, "2026-07-18T03:00:00Z");
    }

    #[test]
    fn every_entity_has_raw_and_unique_names() {
        let mut names = std::collections::HashSet::new();
        for e in ENTITIES {
            assert!(names.insert(e.name), "duplicate entity {}", e.name);
            assert!(
                e.cols.iter().any(|c| matches!(c.ex, Ex::Raw)),
                "{} missing raw column",
                e.name
            );
            assert!(
                e.cols.iter().any(|c| c.pk),
                "{} missing a primary key column",
                e.name
            );
            if e.since {
                assert!(e.cursor.is_some(), "{} since without cursor", e.name);
            }
        }
        assert_eq!(ENTITIES.len(), 10);
    }
}
