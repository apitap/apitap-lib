//! `mode="log_based"`: batch CDC from Postgres — drain a logical
//! replication slot, collapse the window per key, apply set-based.
//! Design: docs/design/log_based.md.

pub(crate) mod collapse;
pub(crate) mod dest_ch;
pub(crate) mod dest_my;
pub(crate) mod dest_pg;
pub(crate) mod drain;
pub(crate) mod rowtext;
pub(crate) mod run;
