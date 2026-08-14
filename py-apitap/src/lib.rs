//! The `apitap._apitap` native module — a thin PyO3 shim over `apitap-core`.
//! The GIL is released for the whole transfer (`allow_threads`), so other Python
//! threads keep running while bytes move.

mod capsule;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::ffi::CStr;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// One shared multi-thread runtime for every call (building one per call would pay
/// thread-spawn latency each time).
fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("tokio runtime"))
}

/// log_based rides a fresh current_thread runtime on the CALLING thread
/// instead of the shared multi-thread one. The CDC pipeline is three tasks
/// (frame pump, decode+collapse, apply) whose Bytes cells share refcounts:
/// on the multi-thread runtime the tasks land on different workers and the
/// atomic increments ping-pong cache lines between cores — a post-knife
/// flamegraph put bytes::shared_clone at 14.4% of samples with an ~18%
/// futex/syscall pool beside it. On one thread the atomics are uncontended
/// and the channel hops never cross cores. I/O overlap survives (tasks
/// interleave cooperatively at every await); only true CPU parallelism is
/// lost, which the capped tier never had — and the historical receipt is
/// that log_based's number "does not move" between the full box and half a
/// core. Bulk lanes keep the shared runtime: their parallel range pipes DO
/// use the cores.
fn run_cdc<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    // Quota-aware: current_thread is a measured win at the half-core tier
    // (RSS −10MB, wall neutral) but it CAPS the pipeline at one thread — a
    // 1-core proof run used only 0.55 cores of a 1.0 quota and gained just
    // 8%. Above ~0.6 core of quota the multi-thread runtime gets the pump,
    // drain and apply genuinely overlapping again.
    if cpu_quota_cores().map_or(true, |q| q <= 0.6) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio current_thread runtime")
            .block_on(fut)
    } else {
        rt().block_on(fut)
    }
}

/// cgroup v2 `cpu.max` ("<quota> <period>" or "max ..."), then v1. `None`
/// means unlimited/unknown — treated as small-tier (the conservative side:
/// current_thread never regressed wall anywhere we measured, multi-thread
/// is only provably better when a real >0.6-core quota exists).
fn cpu_quota_cores() -> Option<f64> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut it = s.split_whitespace();
        if let (Some(q), Some(p)) = (it.next(), it.next()) {
            if q != "max" {
                if let (Ok(q), Ok(p)) = (q.parse::<f64>(), p.parse::<f64>()) {
                    if p > 0.0 {
                        return Some(q / p);
                    }
                }
            }
        }
    }
    let q = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?;
    let p = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?;
    let (q, p) = (q.trim().parse::<f64>().ok()?, p.trim().parse::<f64>().ok()?);
    if q > 0.0 && p > 0.0 {
        Some(q / p)
    } else {
        None
    }
}

/// Returns `(rows, elapsed_ms, parallel)`; the Python wrapper turns it into a report.
#[pyfunction]
#[pyo3(signature = (src, dst, table, *, dest_table=None, parallel=None, cursor=None, chunk_bytes=None, durable=true, mode="replace", engine=None, order_by=None, on_cluster=None, partition_by=None, partition_by_per_table=None, order_by_per_table=None, changelog=false))]
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
    partition_by: Option<String>,
    partition_by_per_table: Option<std::collections::HashMap<String, String>>,
    order_by_per_table: Option<std::collections::HashMap<String, String>>,
    changelog: bool,
) -> PyResult<(u64, u64, usize)> {
    let mode: apitap_core::Mode = mode
        .parse()
        .map_err(|e: apitap_core::Error| PyValueError::new_err(e.to_string()))?;
    let opts = apitap_core::TransferOptions {
        parallel,
        cursor,
        dest_table,
        chunk_bytes,
        durable,
        mode,
        engine,
        order_by,
        on_cluster,
        partition_by,
        partition_by_per_table: partition_by_per_table.unwrap_or_default(),
        order_by_per_table: order_by_per_table.unwrap_or_default(),
        changelog,
    };
    let cdc = matches!(opts.mode, apitap_core::Mode::LogBased);
    let out = py.allow_threads(|| {
        if cdc {
            run_cdc(apitap_core::transfer(&src, &dst, &table, &opts))
        } else {
            rt().block_on(apitap_core::transfer(&src, &dst, &table, &opts))
        }
    });
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
#[pyo3(signature = (src, dst, *, tables=None, schema=None, specs=None, parallel=None, cursor=None, chunk_bytes=None, durable=true, mode="replace", engine=None, order_by=None, on_cluster=None, partition_by=None, partition_by_per_table=None, order_by_per_table=None, changelog=false))]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
fn transfer_many(
    py: Python<'_>,
    src: String,
    dst: String,
    tables: Option<Vec<String>>,
    schema: Option<String>,
    specs: Option<Vec<(String, String)>>,
    parallel: Option<usize>,
    cursor: Option<String>,
    chunk_bytes: Option<usize>,
    durable: bool,
    mode: &str,
    engine: Option<String>,
    order_by: Option<String>,
    on_cluster: Option<String>,
    partition_by: Option<String>,
    partition_by_per_table: Option<std::collections::HashMap<String, String>>,
    order_by_per_table: Option<std::collections::HashMap<String, String>>,
    changelog: bool,
) -> PyResult<(u64, usize, Vec<(String, u64, u64, usize, Option<String>)>)> {
    let mode: apitap_core::Mode = mode
        .parse()
        .map_err(|e: apitap_core::Error| PyValueError::new_err(e.to_string()))?;
    // Per-table modes ({table: mode} on the Python side) parse up front so a
    // typo fails before anything moves.
    let parsed_specs: Option<Vec<(String, apitap_core::Mode)>> = match specs {
        None => None,
        Some(pairs) => Some(
            pairs
                .into_iter()
                .map(|(t, m)| match m.parse::<apitap_core::Mode>() {
                    Ok(m) => Ok((t, m)),
                    Err(e) => Err(PyValueError::new_err(format!("table {t:?}: {e}"))),
                })
                .collect::<PyResult<_>>()?,
        ),
    };
    let opts = apitap_core::TransferOptions {
        parallel,
        cursor,
        dest_table: None,
        chunk_bytes,
        durable,
        mode,
        engine,
        order_by,
        on_cluster,
        partition_by,
        partition_by_per_table: partition_by_per_table.unwrap_or_default(),
        order_by_per_table: order_by_per_table.unwrap_or_default(),
        changelog,
    };
    let cdc = matches!(opts.mode, apitap_core::Mode::LogBased);
    let out = py.allow_threads(|| {
        let fut = async {
            if let Some(sp) = &parsed_specs {
                return apitap_core::transfer_tables(&src, &dst, sp, &opts).await;
            }
            match &tables {
                Some(ts) => apitap_core::transfer_many(&src, &dst, ts, &opts).await,
                None => apitap_core::transfer_schema(&src, &dst, schema.as_deref(), &opts).await,
            }
        };
        if cdc {
            run_cdc(fut)
        } else {
            rt().block_on(fut)
        }
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

static STREAM_NAME: &CStr = c"arrow_array_stream";

unsafe extern "C" fn stream_capsule_destructor(cap: *mut pyo3::ffi::PyObject) {
    let p = pyo3::ffi::PyCapsule_GetPointer(cap, STREAM_NAME.as_ptr());
    if p.is_null() {
        pyo3::ffi::PyErr_Clear();
        return;
    }
    let stream = p as *mut capsule::ArrowArrayStream;
    if let Some(rel) = (*stream).release {
        rel(stream);
    }
    drop(Box::from_raw(stream));
}

/// The native read stream: hand it to any Arrow consumer via
/// `__arrow_c_stream__` (PyCapsule protocol). One-shot — the capsule owns
/// the stream after the first call.
#[pyclass]
struct PgRead {
    stream: Option<usize>, // *mut ArrowArrayStream, kept as usize for Send
    names: Vec<String>,
}

#[pymethods]
impl PgRead {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__(
        &mut self,
        py: Python<'_>,
        requested_schema: Option<PyObject>,
    ) -> PyResult<PyObject> {
        let _ = requested_schema;
        let ptr = self
            .stream
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("read stream already consumed"))?
            as *mut capsule::ArrowArrayStream;
        unsafe {
            let cap = pyo3::ffi::PyCapsule_New(
                ptr as *mut std::ffi::c_void,
                STREAM_NAME.as_ptr(),
                Some(stream_capsule_destructor),
            );
            if cap.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(PyObject::from_owned_ptr(py, cap))
        }
    }

    /// Column names, for repr/debug without consuming the stream.
    fn columns(&self) -> Vec<String> {
        self.names.clone()
    }
}

impl Drop for PgRead {
    fn drop(&mut self) {
        if let Some(p) = self.stream.take() {
            let stream = p as *mut capsule::ArrowArrayStream;
            unsafe {
                if let Some(rel) = (*stream).release {
                    rel(stream);
                }
                drop(Box::from_raw(stream));
            }
        }
    }
}

/// Start a parallel Arrow read; setup errors surface here as normal Python
/// exceptions, before any stream exists.
#[pyfunction]
#[pyo3(signature = (src, table=None, *, cursor=None, parallel=None, query=None, materialize=false, columns=None, push_where=None))]
fn read(
    py: Python<'_>,
    src: String,
    table: Option<String>,
    cursor: Option<String>,
    parallel: Option<usize>,
    query: Option<String>,
    materialize: bool,
    columns: Option<Vec<String>>,
    push_where: Option<String>,
) -> PyResult<PgRead> {
    let table = table.unwrap_or_default();
    let opts = apitap_core::ReadOptions {
        parallel,
        cursor,
        query,
        // to_polars()'s fast path: one giant batch per worker, no
        // mid-stream sealing, minimal FFI crossings.
        batch_bytes: materialize.then_some(usize::MAX >> 1),
        columns,
        push_where,
    };
    let handle = py
        .allow_threads(|| rt().block_on(apitap_core::read_start(&src, &table, &opts)))
        .map_err(|e| match e {
            apitap_core::Error::InvalidInput(m) => PyValueError::new_err(m),
            e => PyRuntimeError::new_err(e.to_string()),
        })?;
    let names = handle.schema.iter().map(|f| f.name.clone()).collect();
    let stream = capsule::new_stream(handle) as usize;
    Ok(PgRead { stream: Some(stream), names })
}

/// Schema-only probe: (name, dtype-tag, nullable) per column, no workers
/// started. Tags: i16 i32 i64 f32 f64 bool date ts:utc ts:naive str bin
/// decimal:<p>:<s> — the Python wrapper maps them onto polars dtypes for
/// the lazy plugin's up-front schema registration.
#[pyfunction]
fn read_schema(
    py: Python<'_>,
    src: String,
    table: String,
) -> PyResult<Vec<(String, String, bool)>> {
    let fields = py
        .allow_threads(|| rt().block_on(apitap_core::read_schema(&src, &table)))
        .map_err(|e| match e {
            apitap_core::Error::InvalidInput(m) => PyValueError::new_err(m),
            e => PyRuntimeError::new_err(e.to_string()),
        })?;
    Ok(fields
        .into_iter()
        .map(|f| {
            use apitap_core::ArrowKind as K;
            let tag = match f.kind {
                K::Int16 => "i16".to_string(),
                K::Int32 => "i32".to_string(),
                K::Int64 => "i64".to_string(),
                K::Float32 => "f32".to_string(),
                K::Float64 => "f64".to_string(),
                K::Bool => "bool".to_string(),
                K::Decimal { p, s } => format!("decimal:{p}:{s}"),
                K::Date32 => "date".to_string(),
                K::TimestampUtc => "ts:utc".to_string(),
                K::TimestampNaive => "ts:naive".to_string(),
                K::Utf8 => "str".to_string(),
                K::Binary => "bin".to_string(),
            };
            (f.name, tag, f.nullable)
        })
        .collect())
}

#[pymodule]
fn _apitap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transfer, m)?)?;
    m.add_function(wrap_pyfunction!(transfer_many, m)?)?;
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(read_schema, m)?)?;
    m.add_class::<PgRead>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
