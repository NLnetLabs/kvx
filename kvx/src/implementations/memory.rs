use std::{
    collections::{BTreeSet, HashMap},
    fmt::Display,
    sync::{Mutex, MutexGuard},
};

use kvx_types::NamespaceBuf;
use lazy_static::lazy_static;

use crate::{
    Error, Key, KeyValueStoreBackend, ReadStore, Result, Scope, TransactionCallback,
    WriteStore,
};

type MemoryStore = HashMap<Key, serde_json::Value>;

lazy_static! {
    static ref STORE: Mutex<MemoryStore> = Mutex::new(MemoryStore::new());
    static ref LOCKS: Mutex<Vec<Scope>> = Mutex::new(Vec::new());
}

#[derive(Debug)]
pub(crate) struct Memory {
    namespace: NamespaceBuf,
    inner: &'static Mutex<MemoryStore>,
    locks: &'static Mutex<Vec<Scope>>,
}

impl Memory {
    pub(crate) fn new(namespace: impl Into<NamespaceBuf>) -> Self {
        Memory {
            namespace: namespace.into(),
            inner: &STORE,
            locks: &LOCKS,
        }
    }

    pub(super) fn lock(&self) -> Result<MutexGuard<'_, MemoryStore>> {
        self.inner
            .lock()
            .map_err(|e| Error::MutexLock(e.to_string()))
    }
}

impl Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyValueStore::Memory({})", self.namespace)
    }
}

struct ReadOnlyMemory {
    inner: MemoryStore,
}

impl ReadStore for ReadOnlyMemory {
    fn has(&self, key: &Key) -> Result<bool> {
        Ok(self.inner.contains_key(key))
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        Ok(self.inner.keys().any(|k| k.scope().starts_with(scope)))
    }

    fn get(&self, key: &Key) -> Result<Option<serde_json::Value>> {
        Ok(self.inner.get(key).cloned())
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        Ok(self
            .inner
            .keys()
            .filter(|k| k.scope().starts_with(scope))
            .cloned()
            .collect::<Vec<Key>>())
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        let scopes: BTreeSet<&Scope> = self
            .inner
            .keys()
            .map(|k| k.scope())
            .collect();

        Ok(scopes.into_iter().cloned().collect())
    }
}

impl KeyValueStoreBackend for Memory {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        // try 10 times to acquire mutex
        for i in 0..10 {
            let mut locks = self
                .locks
                .lock()
                .map_err(|e| Error::MutexLock(e.to_string()))?;

            if locks.iter().any(|s| s.matches(&scope)) {
                if i >= 10 {
                    return Err(Error::MutexLock(format!("Scope {} already locked", scope)));
                } else {
                    drop(locks);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            } else {
                locks.push(scope.clone());
                break;
            }
        }

        callback(self)?;

        let mut locks = self
            .locks
            .lock()
            .map_err(|e| Error::MutexLock(e.to_string()))?;

        locks.retain(|s: &Scope| s != scope);

        Ok(())
    }
}

impl ReadStore for Memory {
    fn has(&self, key: &Key) -> Result<bool> {
        Ok(self.lock()?.contains_key(&key))
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        Ok(self.lock()?.keys().any(|k| k.scope().starts_with(&scope)))
    }

    fn get(&self, key: &Key) -> Result<Option<serde_json::Value>> {
        Ok(self.lock()?.get(&key).cloned())
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        Ok(self
            .lock()?
            .keys()
            .filter(|k| k.scope().starts_with(&scope))
            .cloned()
            .collect::<Vec<Key>>())
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        let scope = Scope::global();
        let scopes: BTreeSet<Scope> = self
            .lock()?
            .keys()
            .filter(|k| k.scope().starts_with(&scope))
            .flat_map(|k| {
                k.scope().sub_scopes()
            })
            .collect();

        Ok(scopes.into_iter().collect())
    }
}

impl WriteStore for Memory {
    fn store(&self, key: &Key, value: serde_json::Value) -> Result<()> {
        self.lock()?.insert(key.clone(), value);
        Ok(())
    }

    fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        let mut inner = self.lock()?;
        if let Some(value) = inner.remove(from) {
            inner.insert(to.clone(), value);
            Ok(())
        } else {
            Err(Error::UnknownKey)
        }
    }

    fn delete(&self, key: &Key) -> Result<()> {
        self.lock()?.remove(key).ok_or(Error::UnknownKey)?;
        Ok(())
    }

    fn delete_scope(&self, scope: &Scope) -> Result<()> {
        self.lock()?.retain(|k, _| !k.scope().starts_with(&scope));
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        let scope = Scope::global();
        self.lock()?.retain(|k, _| !k.scope().starts_with(&scope));
        Ok(())
    }

    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        let mut inner = self.lock()?;
        *inner = inner
            .iter()
            .map(|(k, v)| {
                if k.scope() == from {
                    (Key::new_scoped(to.clone(), k.name()), v.clone())
                } else {
                    (k.clone(), v.clone())
                }
            })
            .collect::<HashMap<Key, serde_json::Value>>();

        Ok(())
    }
}
