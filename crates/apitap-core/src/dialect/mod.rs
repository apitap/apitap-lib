//! Per-database SQL vocabulary (identifier quoting, shared type sets, session
//! setup) used by BOTH that database's source and sink — the one place the two
//! sides must agree.

pub(crate) mod mysql;
pub(crate) mod postgres;
