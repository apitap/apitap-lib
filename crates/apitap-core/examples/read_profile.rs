//! Profiling harness for the Arrow read hot path (no Python layer — the
//! no-op-consumer test showed the cost lives entirely on the decode side).
//!
//!     SRC=postgres://… TABLE=bench_data_10m_cap PARALLEL=5 \
//!         cargo run --release --example read_profile --features hotpath
//!
//! Without the `hotpath` feature this is a plain runner (perf/flamegraph
//! target with zero instrumentation overhead).

use apitap_core::ReadOptions;

#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main)]
async fn main() {
    let src = std::env::var("SRC").expect("SRC connection url");
    let table = std::env::var("TABLE").expect("TABLE");
    let opts = ReadOptions {
        parallel: std::env::var("PARALLEL").ok().and_then(|p| p.parse().ok()),
        ..Default::default()
    };
    let t0 = std::time::Instant::now();
    let mut handle = apitap_core::read_start(&src, &table, &opts)
        .await
        .expect("read_start");
    // Drain on a blocking thread exactly like the Python consumer does.
    let (rows, batches) = tokio::task::spawn_blocking(move || {
        let mut rows = 0u64;
        let mut batches = 0u64;
        while let Some(b) = handle.next_batch().expect("next_batch") {
            rows += b.rows as u64;
            batches += 1;
        }
        (rows, batches)
    })
    .await
    .expect("join");
    println!(
        "{rows} rows in {} batches, {:.1}s",
        batches,
        t0.elapsed().as_secs_f64()
    );
}
