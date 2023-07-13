use std::fmt::{Debug, Display};

use implementations::{disk::Disk, memory::Memory};
#[cfg(feature = "macros")]
pub use kvx_macros::segment;
pub use kvx_types::{Key, Scope, Segment, SegmentBuf};
#[cfg(feature = "queue")]
pub use queue::Queue;
use serde_json::Value;
use url::Url;

pub use crate::error::Error;

mod error;
mod implementations;
#[cfg(feature = "queue")]
mod queue;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// Read operations of a store
pub trait ReadStore {
    fn has(&self, key: &Key) -> Result<bool>;
    fn has_scope(&self, scope: &Scope) -> Result<bool>;
    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>>;
    fn list_scopes(&self) -> Result<Vec<Scope>>;
}

/// Read operations of a store
pub trait WriteStore {
    fn store(&self, key: &Key, value: Value) -> Result<()>;
    fn move_value(&self, from: &Key, to: &Key) -> Result<()>;
    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()>;

    fn delete(&self, key: &Key) -> Result<()>;
    fn delete_scope(&self, scope: &Scope) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

pub(crate) type TransactionCallback<'s> =
    &'s mut dyn FnMut(&dyn KeyValueStoreBackend) -> Result<()>;

/// Read, Write and Transaction operations of a store
pub trait KeyValueStoreBackend: ReadStore + WriteStore {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()>;
}

pub trait PubKeyValueStoreBackend: KeyValueStoreBackend + Debug + Send + Sync + Display {}

impl<T> PubKeyValueStoreBackend for T where T: KeyValueStoreBackend + Debug + Send + Sync + Display {}

/// Represents a key-value store, wraps a backend
///
/// # Example
/// ```
/// use kvx::{Segment, KeyValueStore};
/// use url::Url;
/// // use an in-memory backend
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let store = KeyValueStore::new(&Url::parse("memory://")?, Segment::parse("ns")?)?;
///
/// // use a file backend
/// let store = KeyValueStore::new(&Url::parse("local://tmp")?, Segment::parse("ns")?)?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct KeyValueStore {
    inner: Box<dyn PubKeyValueStoreBackend>,
}

impl KeyValueStore {
    pub fn new(storage_uri: &Url, namespace: impl Into<SegmentBuf>) -> Result<KeyValueStore> {
        let namespace = namespace.into();
        let inner: Box<dyn PubKeyValueStoreBackend> = match storage_uri.scheme() {
            "local" => {
                let path = format!(
                    "{}{}",
                    storage_uri.host_str().unwrap_or_default(),
                    storage_uri.path()
                );

                Box::new(Disk::new(&path, namespace.as_str())?)
            }
            "memory" => Box::new(Memory::new(
                format!(
                    "{} {}",
                    storage_uri.host_str().unwrap_or_default(),
                    namespace
                )
                .parse()
                .unwrap_or(namespace),
            )),
            #[cfg(feature = "postgres")]
            "postgres" => Box::new(crate::implementations::postgres::Postgres::new(
                storage_uri,
                namespace,
            )?),
            scheme => Err(crate::error::Error::UnknownScheme(scheme.to_owned()))?,
        };

        Ok(KeyValueStore { inner })
    }

    pub fn execute<F, T>(&self, scope: &Scope, mut op: F) -> Result<T>
    where
        F: FnMut(&dyn KeyValueStoreBackend) -> Result<T, Error>,
    {
        let mut res = None;
        self.transaction(scope, &mut |store| {
            res = Some(op(store)?);
            Ok(())
        })?;
        Ok(res.unwrap())
    }
}

impl Display for KeyValueStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl KeyValueStoreBackend for KeyValueStore {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        self.inner.transaction(scope, callback)
    }
}

impl ReadStore for KeyValueStore {
    fn has(&self, key: &Key) -> Result<bool> {
        self.inner.has(key)
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        self.inner.has_scope(scope)
    }

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.inner.get(key)
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        self.inner.list_keys(scope)
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        self.inner.list_scopes()
    }
}

impl WriteStore for KeyValueStore {
    fn store(&self, key: &Key, value: Value) -> Result<()> {
        self.inner.store(key, value)
    }

    fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        self.inner.move_value(from, to)
    }

    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        self.inner.move_scope(from, to)
    }

    fn delete(&self, key: &Key) -> Result<()> {
        self.inner.delete(key)
    }

    fn delete_scope(&self, scope: &Scope) -> Result<()> {
        self.inner.delete_scope(scope)
    }

    fn clear(&self) -> Result<()> {
        self.inner.clear()
    }
}
