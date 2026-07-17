//! The `apitap._apitap` native module — a thin PyO3 shim over `apitap-core`.
//! The GIL is released for the whole transfer (`allow_threads`), so other Python
//! threads keep running while bytes move.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// One shared multi-thread runtime for every call (building one per call would pay
/// thread-spawn latency each time).
fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("tokio runtime"))
}

/// Returns `(rows, elapsed_ms, parallel)`; the Python wrapper turns it into a report.
#[pyfunction]
#[pyo3(signature = (src, dst, table, *, dest_table=None, parallel=None, cursor=None, chunk_bytes=None, durable=true, mode="replace", engine=None, order_by=None, on_cluster=None))]
#[allow(clippy::too_many_arguments)]
fn transfer(
    py: Python<'_>,
    src: String,
    dst: String,
    table: String,
    dest_table: Option<String>,
    parallel: Option<usize>,
    cursor: Option<String>,
    chunk_bytes: Option<usize>,
    durable: bool,
    mode: &str,
    engine: Option<String>,
    order_by: Option<String>,
    on_cluster: Option<String>,
) -> PyResult<(u64, u64, usize)> {
    let mode: apitap_core::Mode = mode
        .parse()
        .map_err(|e: apitap_core::Error| PyValueError::new_err(e.to_string()))?;
    let opts = apitap_core::TransferOptions {
        parallel,
        cursor,
        dest_table,
        chunk_bytes: chunk_bytes.unwrap_or(4 * 1024 * 1024),
        durable,
        mode,
        engine,
        order_by,
        on_cluster,
    };
    let out = py.allow_threads(|| rt().block_on(apitap_core::transfer(&src, &dst, &table, &opts)));
    match out {
        Ok(r) => Ok((r.rows, r.elapsed_ms, r.parallel)),
        Err(apitap_core::Error::InvalidInput(m)) => Err(PyValueError::new_err(m)),
        Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
    }
}

/// Multi-table run. Exactly one of `tables`/`schema` is set (the Python wrapper
/// validates). Returns `(elapsed_ms, budget, [(table, rows, elapsed_ms, parallel,
/// error), …])` — per-table failures ride in the rows, not as an exception, so the
/// wrapper can report which tables landed.
#[pyfunction]
#[pyo3(signature = (src, dst, *, tables=None, schema=None, parallel=None, cursor=None, chunk_bytes=None, durable=true, mode="replace", engine=None, order_by=None, on_cluster=None))]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
fn transfer_many(
    py: Python<'_>,
    src: String,
    dst: String,
    tables: Option<Vec<String>>,
    schema: Option<String>,
    parallel: Option<usize>,
    cursor: Option<String>,
    chunk_bytes: Option<usize>,
    durable: bool,
    mode: &str,
    engine: Option<String>,
    order_by: Option<String>,
    on_cluster: Option<String>,
) -> PyResult<(u64, usize, Vec<(String, u64, u64, usize, Option<String>)>)> {
    let mode: apitap_core::Mode = mode
        .parse()
        .map_err(|e: apitap_core::Error| PyValueError::new_err(e.to_string()))?;
    let opts = apitap_core::TransferOptions {
        parallel,
        cursor,
        dest_table: None,
        chunk_bytes: chunk_bytes.unwrap_or(4 * 1024 * 1024),
        durable,
        mode,
        engine,
        order_by,
        on_cluster,
    };
    let out = py.allow_threads(|| {
        rt().block_on(async {
            match &tables {
                Some(ts) => apitap_core::transfer_many(&src, &dst, ts, &opts).await,
                None => apitap_core::transfer_schema(&src, &dst, schema.as_deref(), &opts).await,
            }
        })
    });
    match out {
        Ok(r) => Ok((
            r.elapsed_ms,
            r.budget,
            r.tables
                .into_iter()
                .map(|t| (t.table, t.rows, t.elapsed_ms, t.parallel, t.error))
                .collect(),
        )),
        Err(apitap_core::Error::InvalidInput(m)) => Err(PyValueError::new_err(m)),
        Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
    }
}

#[pymodule]
fn _apitap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transfer, m)?)?;
    m.add_function(wrap_pyfunction!(transfer_many, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
