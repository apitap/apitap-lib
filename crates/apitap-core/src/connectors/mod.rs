//! One connector per database, each exporting its [`crate::driver::Source`] and/or
//! [`crate::driver::Sink`] implementation. Adding a database = adding a file here and
//! registering its URL scheme in [`crate::transfer`].

pub(crate) mod clickhouse;
pub(crate) mod mysql;
pub(crate) mod postgres;
