use std::{fmt::Debug, future::Future};

use async_trait::async_trait;
use serde_json::Value;
use url::Url;

use crate::{Key, Result, Scope, SegmentBuf};

#[async_trait]
pub trait ReadStore {
    async fn has(&self, key: &Key) -> Result<bool>;
    async fn has_scope(&self, scope: &Scope) -> Result<bool>;
    async fn get(&self, key: &Key) -> Result<Option<Value>>;
    async fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>>;
    async fn list_scopes(&self) -> Result<Vec<Scope>>;
}

#[async_trait]
pub trait WriteStore {
    async fn store(&self, key: &Key, value: Value) -> Result<()>;
    async fn move_value(&self, from: &Key, to: &Key) -> Result<()>;
    async fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()>;

    async fn delete(&self, key: &Key) -> Result<()>;
    async fn delete_scope(&self, scope: &Scope) -> Result<()>;
    async fn clear(&self) -> Result<()>;
}

pub type TransactionCallback = Box<
    dyn Fn(
            &(dyn KeyValueStoreBackend + Sync),
        ) -> Box<dyn Future<Output = Result<()>> + Unpin + Send>
        + Send
        + Sync,
>;

#[async_trait]
pub trait KeyValueStoreBackend: ReadStore + WriteStore {
    async fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()>;
}

pub trait PubKeyValueStoreBackend: KeyValueStoreBackend + Debug + Send + Sync {}

impl<T> PubKeyValueStoreBackend for T where T: KeyValueStoreBackend + Debug + Send + Sync {}

#[derive(Debug)]
pub struct KeyValueStore {
    inner: Box<dyn PubKeyValueStoreBackend>,
}

impl KeyValueStore {
    pub fn new(storage_uri: &Url, namespace: impl Into<SegmentBuf>) -> Result<KeyValueStore> {
        let inner: Box<dyn PubKeyValueStoreBackend> = match storage_uri.scheme() {
            #[cfg(feature = "postgres")]
            "postgres" => Box::new(
                crate::nonblocking::implementations::postgres::Postgres::new(
                    storage_uri,
                    namespace,
                )?,
            ),
            scheme => Err(crate::error::Error::UnknownScheme(scheme.to_owned()))?,
        };

        Ok(KeyValueStore { inner })
    }
}

#[async_trait]
impl KeyValueStoreBackend for KeyValueStore {
    async fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        self.inner.transaction(scope, callback).await
    }
}

#[async_trait]
impl ReadStore for KeyValueStore {
    async fn has(&self, key: &Key) -> Result<bool> {
        self.inner.has(key).await
    }

    async fn has_scope(&self, scope: &Scope) -> Result<bool> {
        self.inner.has_scope(scope).await
    }

    async fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.inner.get(key).await
    }

    async fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        self.inner.list_keys(scope).await
    }

    async fn list_scopes(&self) -> Result<Vec<Scope>> {
        self.inner.list_scopes().await
    }
}

#[async_trait]
impl WriteStore for KeyValueStore {
    async fn store(&self, key: &Key, value: Value) -> Result<()> {
        self.inner.store(key, value).await
    }

    async fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        self.inner.move_value(from, to).await
    }

    async fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        self.inner.move_scope(from, to).await
    }

    async fn delete(&self, key: &Key) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn delete_scope(&self, scope: &Scope) -> Result<()> {
        self.inner.delete_scope(scope).await
    }

    async fn clear(&self) -> Result<()> {
        self.inner.clear().await
    }
}
