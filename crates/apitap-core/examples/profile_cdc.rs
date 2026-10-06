//! Profiling harness for the `mode="log_based"` drain (no Python layer).
//!
//!     SRC=postgres://… DST=clickhouse://… TABLE=public.prof_pg_m \
//!         cargo run --release --example profile_cdc -p apitap-core --features hotpath
//!
//! Add `,hotpath-alloc` for the allocation report. Without the `hotpath`
//! feature this builds to a plain runner with zero profiling overhead. One
//! table: the single-table shape isolates the per-change cost; a group adds
//! windows, not per-row work.

use apitap_core::{Mode, TransferOptions};

#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    let src = std::env::var("SRC").expect("SRC connection url");
    let dst = std::env::var("DST").expect("DST connection url");
    let table = std::env::var("TABLE").expect("TABLE, e.g. public.prof_pg_m");
    let mut opts = TransferOptions::default();
    opts.mode = Mode::LogBased;
    let r = apitap_core::transfer(&src, &dst, &table, &opts)
        .await
        .expect("drain failed");
    println!(
        "{} changes in {} ms over {} pipes",
        r.rows, r.elapsed_ms, r.parallel
    );
}
