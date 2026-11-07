//! Profiling harness for `apitap.sql()` (no Python layer): where a query's
//! wall time goes, per phase, waiting on servers included.
//!
//!     SQL_FILE=q3.sql PG=postgres://… MY=mysql://… \
//!         cargo run --release --example sql_profile --features hotpath
//!
//! Add `,hotpath-alloc` for the allocation report instead of the timing one.
//! The tables are named `pg.…` and `my.…` in the SQL.

use std::collections::HashMap;

#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    let sql = std::fs::read_to_string(std::env::var("SQL_FILE").expect("SQL_FILE")).expect("readable SQL_FILE");
    let mut sources = HashMap::new();
    sources.insert("pg".to_string(), std::env::var("PG").expect("PG url"));
    sources.insert("my".to_string(), std::env::var("MY").expect("MY url"));
    let t0 = std::time::Instant::now();
    let r = apitap_core::sql_run(&sql, &sources).await.expect("query failed");
    println!("{} rows in {:.1} s", r.batch.rows, t0.elapsed().as_secs_f64());
}
