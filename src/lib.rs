use std::fmt::Debug;

use implementations::{disk::Disk, memory::Memory};
pub use key::{Key, Scope, Segment};
use serde_json::Value;
use url::Url;

pub use crate::error::Error;

mod error;
mod implementations;
mod key;

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait ReadStore {
    fn has(&mut self, key: &Key) -> Result<bool>;
    fn has_scope(&mut self, scope: &Scope) -> Result<bool>;
    fn get(&mut self, key: &Key) -> Result<Option<Value>>;
    fn list_keys(&mut self, scope: &Scope) -> Result<Vec<Key>>;
    fn list_scopes(&mut self) -> Result<Vec<Scope>>;
}

pub trait WriteStore {
    fn store(&mut self, key: &Key, value: Value) -> Result<()>;
    fn move_value(&mut self, from: &Key, to: &Key) -> Result<()>;
    fn move_scope(&mut self, from: &Scope, to: &Scope) -> Result<()>;

    fn delete(&mut self, key: &Key) -> Result<()>;
    fn delete_scope(&mut self, scope: &Scope) -> Result<()>;
    fn clear(&mut self) -> Result<()>;
}

pub(crate) type TransactionCallback = Box<dyn Fn(&mut dyn KeyValueStoreBackend) -> Result<()>>;

pub trait KeyValueStoreBackend: ReadStore + WriteStore {
    fn transaction(&mut self, scope: &Scope, callback: TransactionCallback) -> Result<()>;
}

pub trait PubKeyValueStoreBackend: KeyValueStoreBackend + Debug + Send + Sync {}

impl<T> PubKeyValueStoreBackend for T where T: KeyValueStoreBackend + Debug + Send + Sync {}

#[derive(Debug)]
pub struct KeyValueStore {
    inner: Box<dyn PubKeyValueStoreBackend>,
}

impl KeyValueStore {
    pub fn new(storage_uri: &Url, namespace: Segment) -> Result<KeyValueStore> {
        let inner: Box<dyn PubKeyValueStoreBackend> = match storage_uri.scheme() {
            "local" => {
                let path = format!(
                    "{}{}",
                    storage_uri.host_str().unwrap_or_default(),
                    storage_uri.path()
                );

                Box::new(Disk::new(&path, namespace.as_str()))
            }
            "memory" => Box::new(Memory::new(namespace)),
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
    fn transaction(&mut self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        self.inner.transaction(scope, callback)
    }
}

impl ReadStore for KeyValueStore {
    fn has(&mut self, key: &Key) -> Result<bool> {
        self.inner.has(key)
    }

    fn has_scope(&mut self, scope: &Scope) -> Result<bool> {
        self.inner.has_scope(scope)
    }

    fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.inner.get(key)
    }

    fn list_keys(&mut self, scope: &Scope) -> Result<Vec<Key>> {
        self.inner.list_keys(scope)
    }

    fn list_scopes(&mut self) -> Result<Vec<Scope>> {
        self.inner.list_scopes()
    }
}

impl WriteStore for KeyValueStore {
    fn store(&mut self, key: &Key, value: Value) -> Result<()> {
        self.inner.store(key, value)
    }

    fn move_value(&mut self, from: &Key, to: &Key) -> Result<()> {
        self.inner.move_value(from, to)
    }

    fn move_scope(&mut self, from: &Scope, to: &Scope) -> Result<()> {
        self.inner.move_scope(from, to)
    }

    fn delete(&mut self, key: &Key) -> Result<()> {
        self.inner.delete(key)
    }

    fn delete_scope(&mut self, scope: &Scope) -> Result<()> {
        self.inner.delete_scope(scope)
    }

    fn clear(&mut self) -> Result<()> {
        self.inner.clear()
    }
}
