//! GitHub repository source:
//! `github://owner/repo[/sub/dir]?ref=main`.
//!
//! The repo (or one directory of it) is the database, its `.csv` FILES are the
//! tables — named by file stem, all-text, streamed; the shared CSV rules live
//! in [`crate::source::csvfile`]. Listing goes through the GitHub API's git
//! trees endpoint (one call per run, scoped to the sub-directory); file bytes
//! stream from the official raw endpoint (CDN, no API quota). `?ref=` pins a
//! branch, tag, or commit SHA — default is the repo's default branch.
//!
//! Auth: `GITHUB_TOKEN` (or `GH_TOKEN`) when set — required for private repos
//! and lifts the API rate limit (60/h anonymous → 5,000/h). The token rides
//! only in the Authorization header, never in a URL.
//!
//! Incremental modes are refused (files carry no usable cursor) — use
//! `mode="replace"`.

use crate::error::{Error, Result};
use crate::plan::{Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::sink::Loader;
use crate::source::csvfile;
use crate::source::Source;
use futures::StreamExt;

const API: &str = "https://api.github.com";
const RAW: &str = "https://raw.githubusercontent.com";
/// Probe reads this much of a file to find the header row.
const PROBE_BYTES: usize = 64 * 1024;

pub(crate) struct GithubSource {
    client: reqwest::Client,
    owner: String,
    repo: String,
    /// Normalized to end with '/' when non-empty.
    prefix: String,
    /// `?ref=` if given; else resolved once from the repo's default branch.
    want_ref: Option<String>,
    token: Option<String>,
    resolved_ref: tokio::sync::OnceCell<String>,
    /// One tree listing per run: catalog, probe and span resolution share it.
    listing: tokio::sync::OnceCell<Vec<(String, i64)>>,
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Percent-encode one path segment (RFC 3986 unreserved rides, rest is %XX).
fn encode_segment(s: &str) -> String {
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

/// Decode one percent-encoded path segment from the user's URL. A decoded '/'
/// or NUL is refused — it would silently change which object is addressed.
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
                    Error::InvalidInput(format!("github url: invalid percent-escape in '{s}'"))
                })?;
            if hex == b'/' || hex == 0 {
                return Err(Error::InvalidInput(format!(
                    "github url: refusing encoded '/' or NUL in path segment '{s}'"
                )));
            }
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|e| Error::InvalidInput(format!("github url path is not UTF-8: {e}")))
}

/// Encode a repo-relative path, keeping '/' between segments.
fn encode_path(p: &str) -> String {
    p.split('/').map(encode_segment).collect::<Vec<_>>().join("/")
}

impl GithubSource {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let u =
            reqwest::Url::parse(url).map_err(|e| Error::InvalidInput(format!("github url: {e}")))?;
        let owner = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "github url needs owner and repo: github://<owner>/<repo>[/dir]?ref=main"
                        .into(),
                )
            })?
            .to_string();
        // Url::path() is percent-ENCODED — decode each segment once, or a
        // directory with a space/unicode would be re-encoded into a literal
        // '%20' lookup (and 404 with a misleading hint).
        let mut segs = u.path().trim_matches('/').splitn(2, '/');
        let repo = decode_component(segs.next().unwrap_or(""))?;
        if repo.is_empty() {
            return Err(Error::InvalidInput(
                "github url needs a repo: github://<owner>/<repo>[/dir]?ref=main".into(),
            ));
        }
        let mut prefix = segs
            .next()
            .unwrap_or("")
            .split('/')
            .map(decode_component)
            .collect::<Result<Vec<_>>>()?
            .join("/");
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let mut want_ref = None;
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "ref" => {
                    if v.is_empty() {
                        return Err(Error::InvalidInput(
                            "github url: ?ref= is empty — pass a branch, tag, or \
                             commit SHA (or drop the parameter for the default \
                             branch)"
                                .into(),
                        ));
                    }
                    want_ref = Some(v.to_string());
                }
                other => {
                    return Err(Error::InvalidInput(format!(
                        "unknown github url parameter '{other}' (supported: ref — auth \
                         goes in the GITHUB_TOKEN env var, never the url)"
                    )))
                }
            }
        }
        Ok(Self {
            client: reqwest::Client::new(),
            owner,
            repo,
            prefix,
            want_ref,
            token: env_nonempty("GITHUB_TOKEN").or_else(|| env_nonempty("GH_TOKEN")),
            resolved_ref: tokio::sync::OnceCell::new(),
            listing: tokio::sync::OnceCell::new(),
        })
    }

    fn req(&self, url: String) -> reqwest::RequestBuilder {
        // GitHub rejects requests without a User-Agent.
        let mut r = self.client.get(url).header("user-agent", "apitap");
        if let Some(t) = &self.token {
            r = r.bearer_auth(t);
        }
        r
    }

    async fn send(&self, url: String, what: &str) -> Result<reqwest::Response> {
        let resp = self
            .req(url)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("github {what}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let hint = match (status.as_u16(), self.token.is_some()) {
                (404, false) => " (a private repo needs GITHUB_TOKEN)",
                (403 | 429, false) => " (rate limited? set GITHUB_TOKEN to lift 60/h → 5,000/h)",
                (403 | 429, true) => " (rate limited — authenticated limits reset hourly)",
                _ => "",
            };
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Transfer(format!(
                "github {what} ({status}){hint}: {}",
                body.chars().take(300).collect::<String>().trim()
            )));
        }
        Ok(resp)
    }

    /// The ref to read: `?ref=` verbatim, else the repo's default branch.
    async fn r#ref(&self) -> Result<&String> {
        self.resolved_ref
            .get_or_try_init(|| async {
                if let Some(r) = &self.want_ref {
                    return Ok(r.clone());
                }
                let v: serde_json::Value = self
                    .send(
                        format!("{API}/repos/{}/{}", self.owner, self.repo),
                        "repo lookup",
                    )
                    .await?
                    .json()
                    .await
                    .map_err(|e| Error::Transfer(format!("github repo response: {e}")))?;
                v["default_branch"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| {
                        Error::Transfer("github repo response missing default_branch".into())
                    })
            })
            .await
    }

    /// Every blob under the prefix: (repo-relative path, size). ONE git-trees
    /// call, scoped with the `ref:path` tree expression so a big repo's
    /// unrelated directories never enter the response.
    async fn listing(&self) -> Result<&Vec<(String, i64)>> {
        self.listing
            .get_or_try_init(|| async {
                let r = self.r#ref().await?;
                let tree_ish = if self.prefix.is_empty() {
                    r.clone()
                } else {
                    format!("{r}:{}", self.prefix.trim_end_matches('/'))
                };
                let v: serde_json::Value = self
                    .send(
                        format!(
                            "{API}/repos/{}/{}/git/trees/{}?recursive=1",
                            self.owner,
                            self.repo,
                            encode_segment(&tree_ish)
                        ),
                        "tree listing",
                    )
                    .await?
                    .json()
                    .await
                    .map_err(|e| Error::Transfer(format!("github tree response: {e}")))?;
                if v["truncated"].as_bool() == Some(true) {
                    return Err(Error::InvalidInput(format!(
                        "github://{}/{}/{} lists too many files for one tree call — \
                         point the url at a narrower directory",
                        self.owner, self.repo, self.prefix
                    )));
                }
                Ok(v["tree"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    // mode 100644/100755 = regular files; 120000 symlinks would
                    // otherwise list as tables and 404 on raw fetch.
                    .filter(|e| {
                        e["type"].as_str() == Some("blob")
                            && e["mode"].as_str().is_some_and(|m| m.starts_with("100"))
                    })
                    .filter_map(|e| {
                        let path = e["path"].as_str()?;
                        Some((
                            format!("{}{path}", self.prefix),
                            e["size"].as_i64().unwrap_or(-1),
                        ))
                    })
                    .collect())
            })
            .await
    }

    async fn tables(&self) -> Result<Vec<(String, String, i64)>> {
        Ok(csvfile::csv_tables(self.listing().await?, &self.prefix))
    }

    fn origin(&self) -> String {
        format!("github://{}/{}/{}", self.owner, self.repo, self.prefix)
    }

    /// stem (or prefix-relative path) → repo path; loud on misses & ambiguity.
    async fn resolve(&self, table: &str) -> Result<String> {
        let tables = self.tables().await?;
        Ok(csvfile::match_table(&tables, &self.prefix, table, &self.origin())?
            .1
            .clone())
    }

    async fn raw(&self, key: &str) -> Result<reqwest::Response> {
        let r = self.r#ref().await?;
        self.send(
            format!(
                "{RAW}/{}/{}/{}/{}",
                self.owner,
                self.repo,
                encode_path(r),
                encode_path(key)
            ),
            "raw fetch",
        )
        .await
    }
}

impl Source for GithubSource {
    fn cursor_quoted(&self, _udt: &str) -> Result<bool> {
        Err(Error::InvalidInput(
            "github files carry no usable incremental cursor — append/merge are not \
             supported; use mode='replace'"
                .into(),
        ))
    }

    async fn probe(&self, table: &str) -> Result<TablePlan> {
        let key = self.resolve(table).await?;
        // Stream until ONE COMPLETE header record parses, then hang up. EOF is
        // only signalled at true end-of-stream — a fabricated EOF would let
        // csv-core flush a truncated header as if it were real.
        let mut stream = self.raw(&key).await?.bytes_stream();
        let mut pump = csvfile::CsvPump::new();
        let mut fed = 0usize;
        let headers: Vec<String> = loop {
            if let Some(row) = pump.next_record(false) {
                break csvfile::row_strings(&row)?;
            }
            if fed > PROBE_BYTES {
                return Err(Error::InvalidInput(format!(
                    "'{key}': header row exceeds {PROBE_BYTES} bytes"
                )));
            }
            match stream.next().await {
                Some(Ok(b)) => {
                    fed += b.len();
                    pump.feed(&b);
                }
                Some(Err(e)) => {
                    return Err(Error::Transfer(format!("github probe '{key}': {e}")))
                }
                None => match pump.next_record(true) {
                    Some(row) => break csvfile::row_strings(&row)?,
                    None => {
                        return Err(Error::InvalidInput(format!(
                            "'{key}' is empty — row 1 must hold the column names"
                        )))
                    }
                },
            }
        };
        drop(stream);
        csvfile::text_plan("github", &key, &headers)
    }

    async fn catalog(
        &self,
        schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        let all = self.tables().await?;
        match tables {
            Some(ts) => {
                let mut out = Vec::with_capacity(ts.len());
                for t in ts {
                    let entry = csvfile::match_table(&all, &self.prefix, t, &self.origin())?;
                    out.push((
                        csvfile::canonical_name(&all, entry, &self.prefix)?,
                        entry.2,
                    ));
                }
                Ok(out)
            }
            None => {
                // schema, for a repo, is an extra DIRECTORY filter under the
                // URL's directory ("*" or nothing = every table), matched on a
                // path-segment boundary: "2026" picks 2026/…, never
                // 2026-backup/… . Duplicate stems don't collide — they get
                // their relative path as the table name (canonical_name).
                let sub = schema
                    .filter(|s| !s.is_empty() && *s != "*")
                    .map(|s| s.trim_end_matches('/'));
                let mut out = Vec::new();
                for entry in &all {
                    let rel = &entry.1[self.prefix.len()..];
                    let keep = match sub {
                        None => true,
                        Some(d) => {
                            rel.len() > d.len()
                                && rel.starts_with(d)
                                && rel.as_bytes()[d.len()] == b'/'
                        }
                    };
                    if keep {
                        out.push((
                            csvfile::canonical_name(&all, entry, &self.prefix)?,
                            entry.2,
                        ));
                    }
                }
                Ok(out)
            }
        }
    }

    fn can_produce(&self, _plan: &TablePlan, format: WireFormat) -> bool {
        // All-text delivery over the shared textrow encodings (same trio as the
        // Google Sheets source).
        matches!(
            format,
            WireFormat::PgCopyBinary | WireFormat::RowBinary | WireFormat::TabSeparated
        )
    }

    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane {
        Lane {
            format,
            cols: plan
                .cols
                .iter()
                .map(|c| LaneCol {
                    delivered: Delivered::Text,
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
        _delta: Option<&Delta>,
    ) -> Result<Vec<String>> {
        // One span: RFC-4180 can't byte-range split (a quoted newline looks
        // identical to a record boundary) — one stream reads the file.
        Ok(vec![self.resolve(table).await?])
    }

    async fn run_workers<L: Loader>(
        &self,
        plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        loaders: Vec<L>,
        chunk: usize,
    ) -> Result<u64> {
        let loader = loaders
            .into_iter()
            .next()
            .expect("one span always yields one loader");
        let key = stmts.into_iter().next().expect("one span statement");
        let stream = match self.raw(&key).await {
            Ok(r) => r.bytes_stream(),
            Err(e) => return Err(loader.abort(e).await),
        };
        csvfile::stream_rows(&key, stream, plan, lane, loader, chunk).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_encoding_is_rfc3986() {
        assert_eq!(encode_path("data/año 1.csv"), "data/a%C3%B1o%201.csv");
        assert_eq!(encode_segment("main:tests/data"), "main%3Atests%2Fdata");
        assert_eq!(encode_path("A-z_0.~"), "A-z_0.~");
    }
}
