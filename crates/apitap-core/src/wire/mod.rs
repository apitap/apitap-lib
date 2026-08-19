//! Pure wire-format encoders/decoders, database-session-free: Postgres binary
//! COPY framing, ClickHouse RowBinary, Parquet for BigQuery load jobs.

pub(crate) mod arrowcol;
pub(crate) mod bqparquet;
pub(crate) mod csvout;
pub(crate) mod mytsv;
pub(crate) mod mybinlog;
pub(crate) mod mywire;
pub(crate) mod pgbindec;
pub(crate) mod pgcopy;
pub(crate) mod pgoutput;
pub(crate) mod walsender;
pub(crate) mod pgmytsv;
pub(crate) mod pgtext;
pub(crate) mod rowbinary;
pub(crate) mod textrow;

// Decoder torture tests — see the module's own note for why this is a
// deterministic harness rather than a fuzzer.
#[cfg(test)]
mod torture;
