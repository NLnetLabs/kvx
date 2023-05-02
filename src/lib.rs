pub use error::{Error, Result};
pub use kvx_macros::segment;
pub use kvx_types::{Key, ParseSegmentError, Scope, Segment, SegmentBuf};
#[cfg(all(feature = "async", feature = "postgres"))]
pub use nonblocking::implementations::postgres;
#[cfg(feature = "async")]
pub use nonblocking::store::{
    KeyValueStore, KeyValueStoreBackend, PubKeyValueStoreBackend, ReadStore, TransactionCallback,
    WriteStore,
};
#[cfg(feature = "queue")]
pub use queue::Queue;
#[cfg(feature = "sync")]
pub mod sync {
    #[cfg(feature = "postgres")]
    pub use super::blocking::implementations::postgres;
    pub use super::blocking::{
        implementations::{disk, memory},
        store::{
            KeyValueStore, KeyValueStoreBackend, PubKeyValueStoreBackend, ReadStore,
            TransactionCallback, WriteStore,
        },
    };
}

#[cfg(feature = "sync")]
mod blocking;
mod error;
#[cfg(feature = "async")]
mod nonblocking;
#[cfg(feature = "queue")]
mod queue;
