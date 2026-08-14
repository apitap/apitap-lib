//! `mode="log_based"`: batch CDC from Postgres — drain a logical
//! replication slot, collapse the window per key, apply set-based.
//! Design: docs/design/log_based.md.

pub(crate) mod changelog;
pub(crate) mod collapse;
pub(crate) mod dest_bq;
pub(crate) mod dest_ch;
pub(crate) mod dest_ice;
pub(crate) mod dest_my;
pub(crate) mod dest_pg;
pub(crate) mod drain;
pub(crate) mod myrun;
pub(crate) mod mysource;
pub(crate) mod resolve;
pub(crate) mod rowtext;
pub(crate) mod run;
