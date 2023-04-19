use std::fmt::Debug;

use implementations::{disk::Disk, memory::Memory};
pub use kvx_macros::segment;
pub use kvx_types::{Key, Scope, Segment, SegmentBuf};
use serde_json::Value;
use url::Url;

pub use crate::error::Error;
// #[cfg(feature = "queue")]
// pub use crate::queue::{Queue, Task};

mod error;
mod implementations;
#[cfg(feature = "queue")]
mod queue;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub trait ReadStore {
    fn has(&self, key: &Key) -> Result<bool>;
    fn has_scope(&self, scope: &Scope) -> Result<bool>;
    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>>;
    fn list_scopes(&self) -> Result<Vec<Scope>>;
}

pub trait WriteStore {
    fn store(&self, key: &Key, value: Value) -> Result<()>;
    fn move_value(&self, from: &Key, to: &Key) -> Result<()>;
    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()>;

    fn delete(&self, key: &Key) -> Result<()>;
    fn delete_scope(&self, scope: &Scope) -> Result<()>;
    fn clear(&self) -> Result<()>;
}

pub(crate) type TransactionCallback = Box<dyn Fn(&dyn KeyValueStoreBackend) -> Result<()>>;

pub trait KeyValueStoreBackend: ReadStore + WriteStore {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()>;
}

pub trait PubKeyValueStoreBackend: KeyValueStoreBackend + Debug + Send + Sync {}

impl<T> PubKeyValueStoreBackend for T where T: KeyValueStoreBackend + Debug + Send + Sync {}

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
