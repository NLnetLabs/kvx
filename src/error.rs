use std::io;

use kvx_types::ParseSegmentError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error {0}")]
    Io(#[from] io::Error),

    #[cfg(feature = "postgres")]
    #[error("postgres error {0}")]
    Postgres(#[from] postgres::Error),

    #[cfg(feature = "postgres")]
    #[error("postgres pool error {0}")]
    PostgresPool(#[from] r2d2_postgres::r2d2::Error),

    #[error("json error {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid segment")]
    Segment(#[from] ParseSegmentError),

    #[error("mutex lock error {0}")]
    MutexLock(String),

    #[error("invalid path {0}")]
    InvalidPath(String),

    #[error("invalid key")]
    InvalidKey,

    #[error("unknown scheme {0}")]
    UnknownScheme(String),

    #[error("unknown error")]
    Unknown,

    #[error("unknown key")]
    UnknownKey,
}
