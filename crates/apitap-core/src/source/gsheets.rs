//! Google Sheets source: `gsheets://<spreadsheet_id>?credentials=/path/key.json`.
//!
//! A spreadsheet is a database, its TABS are the tables — `table="Sheet1"` moves
//! one tab, `tables=[…]`/`schema=` move several through the usual shared budget.
//! Row 1 is the header row (column names); every column arrives as TEXT (sheets
//! are untyped — a typed cast belongs in the destination, where it can fail
//! loudly per value), rendered as the sheet DISPLAYS it (`FORMATTED_VALUE`).
//! Blank cells land as NULL. Reads are a single stream (the Sheets API serves
//! whole ranges; a tab is small by database standards — the API itself caps a
//! spreadsheet at 10M cells), fetched in 10k-row pages.
//!
//! Auth is the service-account JWT flow shared with the BigQuery sink
//! ([`crate::gcp`]), read-only scope. Incremental modes are refused (sheets
//! carry no usable cursor) — use `mode="replace"`.

use crate::error::{Error, Result};
use crate::pipe::{Pipe, PipeBody, Pipes, Rows, Spans};
use crate::plan::{ColumnPlan, Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::sink::Loader;
use crate::source::Source;
use crate::wire::textrow::TextEnc;
use serde_json::Value;

const SHEETS_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets.readonly";
const API: &str = "https://sheets.googleapis.com/v4/spreadsheets";

/// One Sheets GET, retrying a 5xx, a 408 or a 429 (honouring Retry-After) and
/// a transport error. It was one attempt, so a quota 429 — Sheets answers it
/// for per-minute read limits — failed a long sheet's pump halfway (P2).
async fn sheets_get(
    client: &reqwest::Client,
    url: reqwest::Url,
    query: &[(&str, &str)],
    token: &str,
) -> Result<Value> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let sent = client
            .get(url.clone())
            .query(query)
            .bearer_auth(token)
            .timeout(crate::http::control_timeout())
            .send()
            .await;
        let resp = match sent {
            Ok(r)
                if crate::http::store_retryable(r.status())
                    && attempt < crate::http::STORE_ATTEMPTS =>
            {
                let wait = crate::http::store_backoff(
                    attempt,
                    r.headers().get(reqwest::header::RETRY_AFTER),
                );
                tokio::time::sleep(wait).await;
                continue;
            }
            Ok(r) => r,
            Err(_) if attempt < crate::http::STORE_ATTEMPTS => {
                tokio::time::sleep(crate::http::store_backoff(attempt, None)).await;
                continue;
            }
            Err(e) => return Err(Error::Transfer(format!("gsheets request: {e}"))),
        };
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Transfer(format!("gsheets response read: {e}")))?;
        if !status.is_success() {
            return Err(Error::Transfer(format!(
                "gsheets api ({status}): {}",
                body.trim()
            )));
        }
        return serde_json::from_str(&body)
            .map_err(|e| Error::Transfer(format!("gsheets response: {e}")));
    }
}
/// Rows per HTTP fetch: large enough to amortize the round-trip, small enough
/// that a page is a few MB at spreadsheet-typical row widths. Overridable via
/// `APITAP_GSHEETS_PAGE_ROWS` for API-limit tuning (and to exercise paging in
/// tests without a 10k-row sheet).
const FETCH_ROWS: usize = 10_000;

fn fetch_rows() -> usize {
    std::env::var("APITAP_GSHEETS_PAGE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(FETCH_ROWS)
}

pub(crate) struct GsheetsSource {
    client: reqwest::Client,
    token: String,
    spreadsheet: String,
}

impl GsheetsSource {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let u =
            reqwest::Url::parse(url).map_err(|e| crate::urlerr::bad_url("gsheets url", url, e))?;
        // The spreadsheet id rides as the host; ids are [A-Za-z0-9_-] so they
        // survive URL host parsing intact.
        let spreadsheet = u
            .host_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "gsheets url needs a spreadsheet id: \
                     gsheets://<spreadsheet_id>?credentials=/path/key.json"
                        .into(),
                )
            })?
            .to_string();
        let mut credentials_path = None;
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "credentials" => credentials_path = Some(v.to_string()),
                other => {
                    return Err(Error::InvalidInput(format!(
                        "unknown gsheets url parameter '{other}' (supported: credentials)"
                    )))
                }
            }
        }
        let credentials = crate::gcp::read_credentials(credentials_path, "gsheets")?;
        let client = crate::http::client();
        let token = crate::gcp::fetch_access_token(&client, &credentials, SHEETS_SCOPE).await?;
        Ok(Self {
            client,
            token,
            spreadsheet,
        })
    }

    /// `'Tab Name'!A1:B2` — single quotes doubled per the Sheets range grammar.
    fn range(tab: &str, cells: &str) -> String {
        format!("'{}'!{cells}", tab.replace('\'', "''"))
    }

    async fn get_json(&self, path_segment: &str, query: &[(&str, &str)]) -> Result<Value> {
        let mut url = reqwest::Url::parse(&format!("{API}/{}", self.spreadsheet))
            .map_err(|e| Error::Transfer(format!("gsheets url build: {e}")))?;
        if !path_segment.is_empty() {
            // path_segments_mut percent-encodes the range (spaces, '!', quotes).
            url.path_segments_mut()
                .map_err(|_| Error::Transfer("gsheets url build".into()))?
                .push("values")
                .push(path_segment);
        }
        sheets_get(&self.client, url, query, &self.token).await
    }

    /// Fetch a range as rows of display strings. Trailing blank cells are
    /// omitted by the API; callers pad against the header width.
    /// The sheet's ALLOCATED grid rows (`rowCount`) — what the row pump
    /// walks to. The values API omits TRAILING empty rows per request, so a
    /// short or empty page is not end-of-data (review R2-9). A failed request
    /// or an unmatched title used to read as "unknown" (-1), which fell back
    /// to stopping at the first empty page: rows after a blank gap were lost
    /// and the run succeeded (P2). Both are errors now.
    async fn grid_rows(&self, tab: &str) -> Result<i64> {
        let meta = self
            .get_json(
                "",
                &[(
                    "fields",
                    "sheets(properties(title,gridProperties(rowCount)))",
                )],
            )
            .await?;
        grid_rows_of(&meta, tab)
    }

    async fn values(&self, range: &str) -> Result<Vec<Vec<String>>> {
        let v = self
            .get_json(
                range,
                &[
                    ("valueRenderOption", "FORMATTED_VALUE"),
                    ("majorDimension", "ROWS"),
                ],
            )
            .await?;
        let rows = v["values"].as_array().cloned().unwrap_or_default();
        Ok(rows
            .into_iter()
            .map(|row| {
                row.as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|cell| match cell {
                        Value::String(s) => s,
                        Value::Null => String::new(),
                        other => other.to_string(),
                    })
                    .collect()
            })
            .collect())
    }
}

impl Source for GsheetsSource {
    fn cursor_quoted(&self, _udt: &str) -> Result<bool> {
        Err(Error::InvalidInput(
            "google sheets carry no usable incremental cursor — append/merge are \
             not supported; use mode='replace'"
                .into(),
        ))
    }

    async fn probe(&self, table: &str) -> Result<TablePlan> {
        let rows = self.values(&Self::range(table, "1:1")).await?;
        let headers = rows.into_iter().next().unwrap_or_default();
        if headers.is_empty() {
            return Err(Error::InvalidInput(format!(
                "tab '{table}' has an empty header row — row 1 must hold the \
                 column names"
            )));
        }
        let mut seen = std::collections::HashSet::new();
        let mut cols = Vec::with_capacity(headers.len());
        for (i, h) in headers.iter().enumerate() {
            let name = if h.trim().is_empty() {
                format!("col_{}", i + 1)
            } else {
                h.trim().to_string()
            };
            if !seen.insert(name.to_lowercase()) {
                return Err(Error::InvalidInput(format!(
                    "tab '{table}' has a duplicate column header '{name}' — \
                     headers must be unique (rename it in the sheet)"
                )));
            }
            cols.push(ColumnPlan {
                name,
                nullable: true,
                int_pk: false,
                native_ddl: None,
                udt: "text".into(),
                precision: None,
                scale: None,
            });
        }
        Ok(TablePlan {
            engine: "gsheets",
            cols,
            cursor: None,
            pk_cols: Vec::new(),
            cursor_col: None,
        })
    }

    async fn catalog(
        &self,
        _schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        let meta = self
            .get_json(
                "",
                &[(
                    "fields",
                    "sheets(properties(title,gridProperties(rowCount)))",
                )],
            )
            .await?;
        let all: Vec<(String, i64)> = meta["sheets"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|s| {
                let p = &s["properties"];
                let title = p["title"].as_str()?.to_string();
                // rowCount is the GRID size (allocated rows), not the data rows —
                // an over-estimate, which only means an over-ask of pipes that the
                // grant resize hands straight back.
                let est = p["gridProperties"]["rowCount"].as_i64().unwrap_or(-1);
                Some((title, est.saturating_sub(1).max(-1)))
            })
            .collect();
        match tables {
            Some(ts) => {
                let known: std::collections::HashMap<&str, i64> =
                    all.iter().map(|(t, e)| (t.as_str(), *e)).collect();
                let mut out = Vec::with_capacity(ts.len());
                for t in ts {
                    match known.get(t.as_str()) {
                        Some(est) => out.push((t.clone(), *est)),
                        None => {
                            return Err(Error::InvalidInput(format!(
                                "tab '{t}' not found in the spreadsheet (tabs: {})",
                                all.iter()
                                    .map(|(t, _)| t.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )))
                        }
                    }
                }
                Ok(out)
            }
            None => Ok(all),
        }
    }

    fn can_produce(&self, _plan: &TablePlan, format: WireFormat) -> bool {
        // All-text delivery: Postgres binary COPY and ClickHouse RowBinary carry
        // it exactly; TabSeparated is emitted in the MySQL `LOAD DATA` escape
        // dialect ([`crate::wire::mytsv`]) for the MySQL sink — the binary-first
        // sinks never negotiate down to it (their accepts() lists binary first).
        matches!(
            format,
            WireFormat::PgCopyBinary | WireFormat::RowBinary | WireFormat::MyTsv
        )
    }

    fn plan_lane(&self, plan: &TablePlan, format: WireFormat) -> Lane {
        Lane {
            format,
            raw_frames: false,
            push_where: None,
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
        // One span: the API serves ranges, pagination happens inside the worker.
        Ok(vec![table.to_string()])
    }

    async fn run_workers<L: Loader>(
        &self,
        plan: &TablePlan,
        lane: &Lane,
        stmts: Vec<String>,
        pipes: Pipes<L>,
        chunk: usize,
    ) -> Result<u64> {
        pipes
            .run_inline(
                stmts,
                GsBody {
                    src: self,
                    plan,
                    lane,
                    chunk,
                },
            )
            .await
    }
}

/// One single-stream worker: page the tab, frame each row, feed the pipe. The
/// pipe is borrowed, never owned — the engine finishes or aborts it.
struct GsBody<'a> {
    src: &'a GsheetsSource,
    plan: &'a TablePlan,
    lane: &'a Lane,
    chunk: usize,
}

impl<L: Loader> PipeBody<L> for GsBody<'_> {
    async fn run(self, pipe: &mut Pipe<L>, spans: Spans) -> Result<Rows> {
        let tab = spans.next()?.expect("one span statement");
        let ncols = self.plan.cols.len();
        let enc = TextEnc::of(self.lane.format);

        let mut out: Vec<u8> = Vec::with_capacity(self.chunk + 64 * 1024);
        enc.open(&mut out);
        let page_rows = fetch_rows();
        // The values API omits TRAILING empty rows per request: a short or
        // even empty page is NOT end-of-data, and a blank row at a page edge
        // used to end the read and silently drop everything after it
        // (review R2-9). The sheet's allocated row count bounds the walk.
        let grid_rows = self.src.grid_rows(&tab).await?;
        let mut start = 2usize; // row 1 is the header
        let mut rows_sent: u64 = 0;
        loop {
            let Some(next) = next_page_start(start, grid_rows) else {
                break;
            };
            start = next;
            let cells = format!("A{start}:ZZZ{}", start + page_rows - 1);
            let page = self.src.values(&GsheetsSource::range(&tab, &cells)).await?;
            if page.is_empty() && grid_rows < 0 {
                // No grid bound to walk to: emptiness is the only end known.
                break;
            }
            for (j, row) in page.iter().enumerate() {
                if let Some(i) = beyond_header(row, ncols) {
                    return Err(Error::InvalidInput(format!(
                        "gsheets: row {} of tab '{tab}' has a value in column {} but the header row \
                         ends at column {} — the cell would be dropped. Give that column a header \
                         (or clear the cell)",
                        start + j,
                        column_letters(i),
                        column_letters(ncols.saturating_sub(1)),
                    )));
                }
                enc.row_start(ncols, &mut out);
                for i in 0..ncols {
                    let cell = row.get(i).map(String::as_str).unwrap_or("");
                    enc.cell(i, cell.as_bytes(), &mut out);
                }
                enc.row_end(&mut out);
                rows_sent += 1;
                if out.len() >= self.chunk {
                    let buf =
                        std::mem::replace(&mut out, Vec::with_capacity(self.chunk + 64 * 1024));
                    pipe.send(buf).await?;
                }
            }
            start += page_rows;
        }
        enc.close(&mut out);
        if !out.is_empty() {
            pipe.send(out).await?;
        }
        Ok(Rows::LoaderElseOwn(rows_sent))
    }
}

/// The first non-empty cell past the header's width, if any. Sheets omits a
/// trailing blank header, so a data cell in a column with no header used to
/// be dropped without a word (MASTER M-48).
fn beyond_header(row: &[String], ncols: usize) -> Option<usize> {
    row.iter()
        .enumerate()
        .skip(ncols)
        .find(|(_, c)| !c.is_empty())
        .map(|(i, _)| i)
}

/// A 0-based column index in the sheet's own spelling: 0 → A, 26 → AA.
fn column_letters(mut i: usize) -> String {
    let mut s = Vec::new();
    loop {
        s.push(b'A' + (i % 26) as u8);
        if i < 26 {
            break;
        }
        i = i / 26 - 1;
    }
    s.reverse();
    String::from_utf8(s).unwrap_or_default()
}

/// A tab's allocated rows from the spreadsheet metadata. Titles match
/// case-insensitively, as the values API resolves a range's sheet name.
fn grid_rows_of(meta: &serde_json::Value, tab: &str) -> Result<i64> {
    meta["sheets"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| {
            s["properties"]["title"]
                .as_str()
                .is_some_and(|t| t.eq_ignore_ascii_case(tab))
        })
        .and_then(|s| s["properties"]["gridProperties"]["rowCount"].as_i64())
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "gsheets: no tab '{tab}' with a row count in the spreadsheet's metadata"
            ))
        })
}

/// Where the row pump's next range starts, or `None` past the allocated grid
/// (`grid_rows < 0` = unknown: walk until a page comes back empty). Emptiness
/// alone is NOT end-of-data — the API omits trailing blanks per request
/// (review R2-9).
fn next_page_start(start: usize, grid_rows: i64) -> Option<usize> {
    if grid_rows >= 0 && start as i64 > grid_rows {
        None
    } else {
        Some(start)
    }
}

#[cfg(test)]
mod tests {
    /// P2: a 429 is retried (with its Retry-After), a 403 is the answer.
    #[test]
    fn a_throttled_sheets_get_is_retried() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let serve = |answers: Vec<&'static str>| async move {
                let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = l.local_addr().unwrap().port();
                let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let h = hits.clone();
                tokio::spawn(async move {
                    for a in answers {
                        let (mut c, _) = l.accept().await.unwrap();
                        let mut b = [0u8; 4096];
                        let _ = c.read(&mut b).await;
                        h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = c.write_all(a.as_bytes()).await;
                    }
                });
                (reqwest::Url::parse(&format!("http://127.0.0.1:{port}/s")).unwrap(), hits)
            };
            let busy = "HTTP/1.1 429 Too Many\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let ok = "HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n{\"a\":1}";
            let denied = "HTTP/1.1 403 No\r\nContent-Length: 2\r\nConnection: close\r\n\r\nno";
            let c = reqwest::Client::new();
            let (url, hits) = serve(vec![busy, ok]).await;
            assert_eq!(super::sheets_get(&c, url, &[], "t").await.unwrap()["a"], 1);
            assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
            let (url, hits) = serve(vec![denied, ok]).await;
            assert!(super::sheets_get(&c, url, &[], "t").await.is_err());
            assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1, "a 403 is the answer");
        });
    }

    /// P2: the row count comes from the tab the values API would read
    /// (case-insensitive); an unmatched tab is an error, not "unknown".
    #[test]
    fn grid_rows_match_the_tab_or_fail() {
        let meta = serde_json::json!({"sheets": [
            {"properties": {"title": "Other", "gridProperties": {"rowCount": 5}}},
            {"properties": {"title": "Sheet1", "gridProperties": {"rowCount": 1000}}}
        ]});
        assert_eq!(super::grid_rows_of(&meta, "Sheet1").unwrap(), 1000);
        assert_eq!(super::grid_rows_of(&meta, "sheet1").unwrap(), 1000);
        assert!(super::grid_rows_of(&meta, "Missing").is_err());
    }

    /// MASTER M-48: a value past the header's width is found and named;
    /// trailing blanks (the API pads nothing, but a user's sheet may) are not.
    #[test]
    fn a_cell_beyond_the_header_is_found_and_named() {
        let row = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(super::beyond_header(&row(&["a", "b"]), 2), None);
        assert_eq!(super::beyond_header(&row(&["a", "b", ""]), 2), None);
        assert_eq!(super::beyond_header(&row(&["a", "b", "", "x"]), 2), Some(3));
        assert_eq!(super::beyond_header(&row(&["a"]), 2), None);
        assert_eq!(super::column_letters(0), "A");
        assert_eq!(super::column_letters(3), "D");
        assert_eq!(super::column_letters(25), "Z");
        assert_eq!(super::column_letters(26), "AA");
        assert_eq!(super::column_letters(701), "ZZ");
        assert_eq!(super::column_letters(702), "AAA");
    }

    /// R2-9: the walk stops only past the allocated grid; an unknown grid
    /// walks on (the caller handles emptiness).
    #[test]
    fn the_page_walk_is_bounded_by_the_grid_not_emptiness() {
        assert_eq!(super::next_page_start(2, 1000), Some(2));
        assert_eq!(super::next_page_start(1000, 1000), Some(1000));
        assert_eq!(super::next_page_start(1001, 1000), None);
        assert_eq!(super::next_page_start(99999, -1), Some(99999));
    }
}
