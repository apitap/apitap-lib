//! Profiling harness for the transfer hot path (no Python layer).
//!
//!     SRC=postgres://… DST=postgres://… TABLE=public.bench_data_10m \
//!         cargo run --release --example profile --features hotpath
//!
//! Add `,hotpath-alloc` to the features for the allocation report instead of
//! the timing one. Without the `hotpath` feature this builds to a plain runner
//! with zero profiling overhead.

use apitap_core::TransferOptions;

#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    let src = std::env::var("SRC").expect("SRC connection url");
    let dst = std::env::var("DST").expect("DST connection url");
    let table = std::env::var("TABLE").expect("TABLE, e.g. public.bench_data_10m");
    let opts = TransferOptions::default();
    let r = apitap_core::transfer(&src, &dst, &table, &opts)
        .await
        .expect("transfer failed");
    println!(
        "{} rows in {} ms over {} pipes",
        r.rows, r.elapsed_ms, r.parallel
    );
}
