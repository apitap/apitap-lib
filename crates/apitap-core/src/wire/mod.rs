//! Pure wire-format encoders/decoders, database-session-free: Postgres binary
//! COPY framing, ClickHouse RowBinary, Parquet for BigQuery load jobs.

pub(crate) mod bqparquet;
pub(crate) mod csvout;
pub(crate) mod mytsv;
pub(crate) mod pgcopy;
pub(crate) mod pgmytsv;
pub(crate) mod pgtext;
pub(crate) mod rowbinary;
pub(crate) mod textrow;
