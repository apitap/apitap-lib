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
use crate::plan::{ColumnPlan, Delivered, Delta, Lane, LaneCol, TablePlan, WireFormat};
use crate::sink::Loader;
use crate::source::Source;
use crate::wire::mytsv::tsv_escape;
use crate::wire::pgcopy as pgc;
use crate::wire::rowbinary::varint;
use serde_json::Value;

const SHEETS_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets.readonly";
const API: &str = "https://sheets.googleapis.com/v4/spreadsheets";
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
        let u = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidInput(format!("gsheets url: {e}")))?;
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
        let client = reqwest::Client::new();
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
        let resp = self
            .client
            .get(url)
            .query(query)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::Transfer(format!("gsheets request: {e}")))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(Error::Transfer(format!(
                "gsheets api ({status}): {}",
                body.trim()
            )));
        }
        serde_json::from_str(&body).map_err(|e| Error::Transfer(format!("gsheets response: {e}")))
    }

    /// Fetch a range as rows of display strings. Trailing blank cells are
    /// omitted by the API; callers pad against the header width.
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
        })
    }

    async fn catalog(
        &self,
        _schema: Option<&str>,
        tables: Option<&[String]>,
    ) -> Result<Vec<(String, i64)>> {
        let meta = self
            .get_json("", &[("fields", "sheets(properties(title,gridProperties(rowCount)))")])
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
        // One span: the API serves ranges, pagination happens inside the worker.
        Ok(vec![table.to_string()])
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
        let tab = stmts.into_iter().next().expect("one span statement");
        let ncols = plan.cols.len();
        #[derive(Clone, Copy, PartialEq)]
        enum Enc {
            Pg,
            RowBin,
            Tsv,
        }
        let enc = match lane.format {
            WireFormat::PgCopyBinary => Enc::Pg,
            WireFormat::RowBinary => Enc::RowBin,
            WireFormat::TabSeparated => Enc::Tsv,
        };

        let mut out: Vec<u8> = Vec::with_capacity(chunk + 64 * 1024);
        if enc == Enc::Pg {
            pgc::header(&mut out);
        }
        let page_rows = fetch_rows();
        let mut start = 2usize; // row 1 is the header
        let mut rows_sent: u64 = 0;
        loop {
            let cells = format!("A{start}:ZZZ{}", start + page_rows - 1);
            let page = match self.values(&Self::range(&tab, &cells)).await {
                Ok(p) => p,
                Err(e) => return Err(loader.abort(e).await),
            };
            if page.is_empty() {
                break;
            }
            let got = page.len();
            for row in &page {
                if enc == Enc::Pg {
                    pgc::tuple_start(ncols, &mut out);
                }
                for i in 0..ncols {
                    let cell = row.get(i).map(String::as_str).unwrap_or("");
                    match enc {
                        Enc::Pg => {
                            if cell.is_empty() {
                                pgc::null_field(&mut out);
                            } else {
                                pgc::field(cell.as_bytes(), &mut out);
                            }
                        }
                        // RowBinary Nullable(String): null flag, then varint+bytes.
                        Enc::RowBin => {
                            if cell.is_empty() {
                                out.push(1);
                            } else {
                                out.push(0);
                                varint(cell.len() as u64, &mut out);
                                out.extend_from_slice(cell.as_bytes());
                            }
                        }
                        // MySQL LOAD DATA line: \t between fields, \n after, \N NULLs.
                        Enc::Tsv => {
                            if i > 0 {
                                out.push(b'\t');
                            }
                            if cell.is_empty() {
                                out.extend_from_slice(b"\\N");
                            } else {
                                tsv_escape(cell.as_bytes(), &mut out);
                            }
                        }
                    }
                }
                if enc == Enc::Tsv {
                    out.push(b'\n');
                }
                rows_sent += 1;
                if out.len() >= chunk {
                    let buf = std::mem::replace(&mut out, Vec::with_capacity(chunk + 64 * 1024));
                    loader.send(buf).await?;
                }
            }
            if got < page_rows {
                break;
            }
            start += got;
        }
        if enc == Enc::Pg {
            pgc::trailer(&mut out);
        }
        if !out.is_empty() {
            loader.send(out).await?;
        }
        let reported = loader.finish().await?;
        Ok(if reported > 0 { reported } else { rows_sent })
    }
}
