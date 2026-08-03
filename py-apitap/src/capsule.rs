//! Hand-rolled Arrow C Data Interface + PyCapsule export — no arrow-rs, no
//! pyarrow. The consumer (polars ≥1.5 / pyarrow ≥14 / duckdb ≥1.1) receives
//! a PyCapsule named "arrow_array_stream" wrapping an ArrowArrayStream whose
//! get_next PULLS batches from the core ReadHandle — memory stays at the
//! in-flight batches, exactly the read path's contract.
//!
//! Ownership rules this file lives and dies by (CDataInterface.html):
//! - every pointer a struct exposes is kept alive by its private_data and
//!   freed ONLY by that struct's release callback;
//! - consumers may bitwise-move the structs and call release from any
//!   thread; release sets `release = None` when done;
//! - end-of-stream = get_next writes an array whose release is None.

use apitap_core::{ArrowBatch, ArrowField, ArrowKind, FinishedCol, ReadHandle};
use std::ffi::{c_char, c_int, c_void, CString};
use std::ptr::{null, null_mut};

pub const ARROW_FLAG_NULLABLE: i64 = 2;

#[repr(C)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

#[repr(C)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

#[repr(C)]
pub struct ArrowArrayStream {
    pub get_schema:
        Option<unsafe extern "C" fn(*mut ArrowArrayStream, *mut ArrowSchema) -> c_int>,
    pub get_next:
        Option<unsafe extern "C" fn(*mut ArrowArrayStream, *mut ArrowArray) -> c_int>,
    pub get_last_error: Option<unsafe extern "C" fn(*mut ArrowArrayStream) -> *const c_char>,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArrayStream)>,
    pub private_data: *mut c_void,
}

// ── schema export ───────────────────────────────────────────────────────────

struct SchemaPrivate {
    /// Owns runtime-built format/name CStrings and the child pointer array.
    _strings: Vec<CString>,
    child_ptrs: Vec<*mut ArrowSchema>,
}

unsafe extern "C" fn release_schema(s: *mut ArrowSchema) {
    if s.is_null() || (*s).release.is_none() {
        return;
    }
    let n = (*s).n_children;
    if !(*s).children.is_null() {
        for i in 0..n {
            let child = *(*s).children.add(i as usize);
            if !child.is_null() {
                if let Some(rel) = (*child).release {
                    rel(child);
                }
                drop(Box::from_raw(child));
            }
        }
    }
    if !(*s).private_data.is_null() {
        drop(Box::from_raw((*s).private_data as *mut SchemaPrivate));
    }
    (*s).release = None;
}

/// The static format string for a kind, or None when it needs runtime build.
fn static_format(kind: ArrowKind) -> Option<&'static std::ffi::CStr> {
    Some(match kind {
        ArrowKind::Int16 => c"s",
        ArrowKind::Int32 => c"i",
        ArrowKind::Int64 => c"l",
        ArrowKind::Float32 => c"f",
        ArrowKind::Float64 => c"g",
        ArrowKind::Bool => c"b",
        ArrowKind::Date32 => c"tdD",
        ArrowKind::TimestampUtc => c"tsu:UTC",
        ArrowKind::TimestampNaive => c"tsu:",
        ArrowKind::Utf8 => c"u",
        ArrowKind::Binary => c"z",
        ArrowKind::Decimal { .. } => return None,
    })
}

/// Write a fresh deep copy of the struct-of-columns schema into `out`.
pub fn export_schema(fields: &[ArrowField], out: *mut ArrowSchema) {
    let mut strings: Vec<CString> = Vec::new();
    let mut child_ptrs: Vec<*mut ArrowSchema> = Vec::with_capacity(fields.len());
    for f in fields {
        let format_ptr: *const c_char = match static_format(f.kind) {
            Some(cs) => cs.as_ptr(),
            None => {
                let ArrowKind::Decimal { p, s } = f.kind else { unreachable!() };
                let cs = CString::new(format!("d:{p},{s}")).expect("no NUL");
                let p = cs.as_ptr();
                strings.push(cs);
                p
            }
        };
        let name = CString::new(f.name.as_str()).unwrap_or_default();
        let name_ptr = name.as_ptr();
        strings.push(name);
        let child = Box::new(ArrowSchema {
            format: format_ptr,
            name: name_ptr,
            metadata: null(),
            flags: if f.nullable { ARROW_FLAG_NULLABLE } else { 0 },
            n_children: 0,
            children: null_mut(),
            dictionary: null_mut(),
            // Children are released (recursively) by the PARENT's release;
            // marking them released-by-parent with a no-op keeps consumers
            // that probe `release != None` happy.
            release: Some(release_schema_child_noop),
            private_data: null_mut(),
        });
        child_ptrs.push(Box::into_raw(child));
    }
    let priv_ = Box::new(SchemaPrivate { _strings: strings, child_ptrs });
    let priv_ptr = Box::into_raw(priv_);
    unsafe {
        (*out).format = c"+s".as_ptr();
        (*out).name = null();
        (*out).metadata = null();
        (*out).flags = 0;
        (*out).n_children = (*priv_ptr).child_ptrs.len() as i64;
        (*out).children = (*priv_ptr).child_ptrs.as_mut_ptr();
        (*out).dictionary = null_mut();
        (*out).release = Some(release_schema);
        (*out).private_data = priv_ptr as *mut c_void;
    }
}

unsafe extern "C" fn release_schema_child_noop(s: *mut ArrowSchema) {
    if !s.is_null() {
        (*s).release = None;
    }
}

// ── array export ────────────────────────────────────────────────────────────

struct ArrayPrivate {
    _buffers: Vec<*const c_void>,
    child_ptrs: Vec<*mut ArrowArray>,
    /// Owns the Vec backing memory behind every buffer pointer.
    _keep: Vec<Box<dyn std::any::Any + Send>>,
}

unsafe extern "C" fn release_array(a: *mut ArrowArray) {
    if a.is_null() || (*a).release.is_none() {
        return;
    }
    if !(*a).children.is_null() {
        for i in 0..(*a).n_children {
            let child = *(*a).children.add(i as usize);
            if !child.is_null() {
                if let Some(rel) = (*child).release {
                    rel(child);
                }
                drop(Box::from_raw(child));
            }
        }
    }
    if !(*a).private_data.is_null() {
        drop(Box::from_raw((*a).private_data as *mut ArrayPrivate));
    }
    (*a).release = None;
}

fn export_col(col: FinishedCol, rows: usize) -> *mut ArrowArray {
    let mut keep: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
    let mut bufs: Vec<*const c_void> = Vec::with_capacity(3);
    let mut null_count: i64 = 0;

    fn validity_ptr(
        v: Option<Vec<u8>>,
        rows: usize,
        keep: &mut Vec<Box<dyn std::any::Any + Send>>,
        null_count: &mut i64,
    ) -> *const c_void {
        match v {
            None => {
                *null_count = 0;
                null()
            }
            Some(bits) => {
                let mut nulls = 0i64;
                for i in 0..rows {
                    if bits[i / 8] & (1 << (i % 8)) == 0 {
                        nulls += 1;
                    }
                }
                *null_count = nulls;
                let p = bits.as_ptr() as *const c_void;
                keep.push(Box::new(bits));
                p
            }
        }
    }

    macro_rules! fixed {
        ($v:expr, $data:expr) => {{
            bufs.push(validity_ptr($v, rows, &mut keep, &mut null_count));
            let p = $data.as_ptr() as *const c_void;
            keep.push(Box::new($data));
            bufs.push(p);
        }};
    }
    macro_rules! varlen {
        ($v:expr, $offsets:expr, $data:expr) => {{
            bufs.push(validity_ptr($v, rows, &mut keep, &mut null_count));
            let po = $offsets.as_ptr() as *const c_void;
            keep.push(Box::new($offsets));
            bufs.push(po);
            let pd = $data.as_ptr() as *const c_void;
            keep.push(Box::new($data));
            bufs.push(pd);
        }};
    }

    match col {
        FinishedCol::I16 { validity, data } => fixed!(validity, data),
        FinishedCol::I32 { validity, data } => fixed!(validity, data),
        FinishedCol::I64 { validity, data } => fixed!(validity, data),
        FinishedCol::F32 { validity, data } => fixed!(validity, data),
        FinishedCol::F64 { validity, data } => fixed!(validity, data),
        FinishedCol::Bool { validity, data } => fixed!(validity, data),
        FinishedCol::Dec128 { validity, data } => fixed!(validity, data),
        FinishedCol::Utf8 { validity, offsets, data } => varlen!(validity, offsets, data),
        FinishedCol::Bin { validity, offsets, data } => varlen!(validity, offsets, data),
    }

    let n_buffers = bufs.len() as i64;
    let priv_ = Box::new(ArrayPrivate { _buffers: bufs, child_ptrs: Vec::new(), _keep: keep });
    let priv_ptr = Box::into_raw(priv_);
    let arr = Box::new(ArrowArray {
        length: rows as i64,
        null_count,
        offset: 0,
        n_buffers,
        n_children: 0,
        buffers: unsafe { (*priv_ptr)._buffers.as_ptr() as *mut *const c_void },
        children: null_mut(),
        dictionary: null_mut(),
        // The REAL release: a column's data buffers live in ITS private_data
        // and must be freed when the parent's release walks the children (a
        // no-op here leaked every batch — found by the 256 MB probe).
        release: Some(release_array),
        private_data: priv_ptr as *mut c_void,
    });
    Box::into_raw(arr)
}

/// Export one batch as a top-level struct array (children = columns).
pub fn export_batch(batch: ArrowBatch, out: *mut ArrowArray) {
    let rows = batch.rows;
    let child_ptrs: Vec<*mut ArrowArray> =
        batch.cols.into_iter().map(|c| export_col(c, rows)).collect();
    // A struct array has ONE buffer slot (its own validity) — NULL here.
    let bufs: Vec<*const c_void> = vec![null()];
    let priv_ = Box::new(ArrayPrivate { _buffers: bufs, child_ptrs, _keep: Vec::new() });
    let priv_ptr = Box::into_raw(priv_);
    unsafe {
        (*out).length = rows as i64;
        (*out).null_count = 0;
        (*out).offset = 0;
        (*out).n_buffers = 1;
        (*out).n_children = (*priv_ptr).child_ptrs.len() as i64;
        (*out).buffers = (*priv_ptr)._buffers.as_ptr() as *mut *const c_void;
        (*out).children = (*priv_ptr).child_ptrs.as_mut_ptr();
        (*out).dictionary = null_mut();
        (*out).release = Some(release_array);
        (*out).private_data = priv_ptr as *mut c_void;
    }
}

// ── the stream ──────────────────────────────────────────────────────────────

pub struct StreamState {
    pub handle: ReadHandle,
    pub schema: Vec<ArrowField>,
    pub last_error: Option<CString>,
}

unsafe extern "C" fn stream_get_schema(
    s: *mut ArrowArrayStream,
    out: *mut ArrowSchema,
) -> c_int {
    let st = &mut *((*s).private_data as *mut StreamState);
    export_schema(&st.schema, out);
    0
}

unsafe extern "C" fn stream_get_next(s: *mut ArrowArrayStream, out: *mut ArrowArray) -> c_int {
    let st = &mut *((*s).private_data as *mut StreamState);
    match st.handle.next_batch() {
        Ok(Some(batch)) => {
            export_batch(batch, out);
            0
        }
        Ok(None) => {
            // End of stream: a released (empty) array.
            std::ptr::write(
                out,
                ArrowArray {
                    length: 0,
                    null_count: 0,
                    offset: 0,
                    n_buffers: 0,
                    n_children: 0,
                    buffers: null_mut(),
                    children: null_mut(),
                    dictionary: null_mut(),
                    release: None,
                    private_data: null_mut(),
                },
            );
            0
        }
        Err(e) => {
            st.last_error = CString::new(e.to_string()).ok();
            5 // EIO
        }
    }
}

unsafe extern "C" fn stream_get_last_error(s: *mut ArrowArrayStream) -> *const c_char {
    let st = &mut *((*s).private_data as *mut StreamState);
    st.last_error.as_ref().map_or(null(), |c| c.as_ptr())
}

unsafe extern "C" fn stream_release(s: *mut ArrowArrayStream) {
    if s.is_null() || (*s).release.is_none() {
        return;
    }
    if !(*s).private_data.is_null() {
        let mut st = Box::from_raw((*s).private_data as *mut StreamState);
        st.handle.cancel();
        drop(st);
    }
    (*s).release = None;
}

/// Heap-allocate the C stream over a finished [`ReadHandle`].
pub fn new_stream(handle: ReadHandle) -> *mut ArrowArrayStream {
    let schema = handle.schema.clone();
    let state = Box::new(StreamState { handle, schema, last_error: None });
    Box::into_raw(Box::new(ArrowArrayStream {
        get_schema: Some(stream_get_schema),
        get_next: Some(stream_get_next),
        get_last_error: Some(stream_get_last_error),
        release: Some(stream_release),
        private_data: Box::into_raw(state) as *mut c_void,
    }))
}
