//! Pure wire-format encoders/decoders, database-session-free: Postgres binary
//! COPY framing, ClickHouse RowBinary, Parquet for BigQuery load jobs.

pub(crate) mod arrowcol;
pub(crate) mod bqparquet;
pub(crate) mod csvout;
pub(crate) mod mybinlog;
pub(crate) mod mytsv;
pub(crate) mod mywire;
pub(crate) mod pgbindec;
pub(crate) mod pgcopy;
pub(crate) mod pgmytsv;
pub(crate) mod pgoutput;
pub(crate) mod pgtext;
pub(crate) mod rowbinary;
pub(crate) mod textrow;
pub(crate) mod walsender;

// Decoder torture tests — see the module's own note for why this is a
// deterministic harness rather than a fuzzer.
#[cfg(test)]
mod torture;
