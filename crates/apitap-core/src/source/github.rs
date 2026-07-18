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
        let mut segs = u.path().trim_matches('/').splitn(2, '/');
        let repo = segs
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "github url needs a repo: github://<owner>/<repo>[/dir]?ref=main".into(),
                )
            })?
            .to_string();
        let mut prefix = segs.next().unwrap_or("").to_string();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let mut want_ref = None;
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "ref" => want_ref = Some(v.to_string()),
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
            let hint = if status.as_u16() == 404 && self.token.is_none() {
                " (a private repo needs GITHUB_TOKEN)"
            } else if status.as_u16() == 403 {
                " (rate limited? set GITHUB_TOKEN to lift 60/h → 5,000/h)"
            } else {
                ""
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
                    .filter(|e| e["type"].as_str() == Some("blob"))
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
        csvfile::csv_tables(self.listing().await?, &self.prefix)
    }

    /// stem (or prefix-relative path) → repo path, loudly listing what exists.
    async fn resolve(&self, table: &str) -> Result<String> {
        let tables = self.tables().await?;
        if let Some((_, key, _)) = tables
            .iter()
            .find(|(stem, key, _)| stem == table || key[self.prefix.len()..] == *table)
        {
            return Ok(key.clone());
        }
        Err(Error::InvalidInput(format!(
            "table '{table}' not found under github://{}/{}/{} (tables: {})",
            self.owner,
            self.repo,
            self.prefix,
            tables
                .iter()
                .map(|(s, _, _)| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )))
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
        // Stream just enough for the header, then hang up — raw files can be
        // huge and Range support is the CDN's business, not a contract.
        let mut stream = self.raw(&key).await?.bytes_stream();
        let mut head: Vec<u8> = Vec::with_capacity(PROBE_BYTES);
        while head.len() < PROBE_BYTES {
            match stream.next().await {
                Some(Ok(b)) => head.extend_from_slice(&b),
                Some(Err(e)) => return Err(Error::Transfer(format!("github probe '{key}': {e}"))),
                None => break,
            }
        }
        drop(stream);
        if head.len() >= PROBE_BYTES && !head.contains(&b'\n') {
            return Err(Error::InvalidInput(format!(
                "'{key}': header row exceeds {PROBE_BYTES} bytes"
            )));
        }
        let headers = csvfile::header_fields(&head[..head.len().min(PROBE_BYTES)])?;
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
                    match all
                        .iter()
                        .find(|(stem, key, _)| stem == t || key[self.prefix.len()..] == *t)
                    {
                        Some((stem, _, est)) => out.push((stem.clone(), *est)),
                        None => {
                            return Err(Error::InvalidInput(format!(
                                "table '{t}' not found under github://{}/{}/{} (tables: {})",
                                self.owner,
                                self.repo,
                                self.prefix,
                                all.iter()
                                    .map(|(s, _, _)| s.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )))
                        }
                    }
                }
                Ok(out)
            }
            None => {
                // schema, for a repo, is an extra path filter under the URL's
                // directory; "*" (or nothing) means every table.
                let sub = schema.filter(|s| !s.is_empty() && *s != "*");
                Ok(all
                    .into_iter()
                    .filter(|(_, key, _)| {
                        sub.is_none_or(|s| key[self.prefix.len()..].starts_with(s))
                    })
                    .map(|(stem, _, est)| (stem, est))
                    .collect())
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
