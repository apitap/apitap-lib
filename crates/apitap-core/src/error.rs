use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// Connection / URL problems (bad DSN, unreachable host, auth).
    #[error("connect: {0}")]
    Connect(String),
    /// A failure inside the transfer itself (COPY stream, staging DDL, swap).
    #[error("transfer: {0}")]
    Transfer(String),
    /// The caller asked for something invalid (unknown table, non-numeric cursor…).
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, Error>;
