use std::io;

use kvx_types::ParseSegmentError;

/// Represents all ways a method can fail within KVx.
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

    /// [`Key`] has an invalid (form)
    ///
    /// [`Key`]: ../kvx/struct.Key.html
    #[error("invalid key")]
    InvalidKey,

    /// Scheme of [`Url`] not known
    ///
    /// [`Url`]: https://docs.rs/url/latest/url/struct.Url.html
    #[error("unknown scheme {0}")]
    UnknownScheme(String),

    #[error("unknown error")]
    Unknown,

    /// Used [`Key`] not known
    ///
    /// [`Key`]: ../kvx/struct.Key.html
    #[error("unknown key")]
    UnknownKey,

    /// Namespace migration issue
    #[error("namespace migration issue: {0}")]
    NamespaceMigration(String),
}
