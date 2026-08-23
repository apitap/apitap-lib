use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    /// Connection / URL problems (bad DSN, unreachable host, auth).
    /// Another run holds this destination table. A distinct variant on
    /// purpose: a scheduler can branch on the type and back off, where a
    /// message it has to regex-match is not an interface.
    #[error("locked: {0}")]
    Locked(String),
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
