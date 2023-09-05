use std::{
    collections::{BTreeSet, HashMap},
    fmt::Display,
    str::FromStr,
    sync::{Mutex, MutexGuard},
};

use kvx_types::NamespaceBuf;
use lazy_static::lazy_static;

use crate::{
    Error, Key, KeyValueStoreBackend, ReadStore, Result, Scope, TransactionCallback, WriteStore,
};

#[derive(Debug)]
pub struct MemoryStore(HashMap<NamespaceBuf, HashMap<Key, serde_json::Value>>);

impl MemoryStore {
    fn new() -> Self {
        MemoryStore(HashMap::new())
    }

    fn has(&self, namespace: &NamespaceBuf, key: &Key) -> bool {
        self.0
            .get(namespace)
            .map(|m| m.contains_key(key))
            .unwrap_or_default()
    }

    fn namespace_is_empty(&self, namespace: &NamespaceBuf) -> bool {
        self.0.get(namespace).map(|m| m.is_empty()).unwrap_or(true)
    }

    fn has_scope(&self, namespace: &NamespaceBuf, scope: &Scope) -> bool {
        self.0
            .get(namespace)
            .map(|m| m.keys().any(|k| k.scope().starts_with(scope)))
            .unwrap_or_default()
    }

    fn get(&self, namespace: &NamespaceBuf, key: &Key) -> Option<serde_json::Value> {
        self.0.get(namespace).and_then(|m| m.get(key).cloned())
    }

    fn insert(&mut self, namespace: &NamespaceBuf, key: &Key, value: serde_json::Value) {
        let map = self.0.entry(namespace.clone()).or_insert_with(HashMap::new);
        map.insert(key.clone(), value);
    }

    fn delete(&mut self, namespace: &NamespaceBuf, key: &Key) -> Result<()> {
        self.0
            .get_mut(namespace)
            .ok_or(Error::UnknownKey)?
            .remove(key)
            .ok_or(Error::UnknownKey)?;
        Ok(())
    }

    fn move_value(&mut self, namespace: &NamespaceBuf, from: &Key, to: &Key) -> Result<()> {
        match self.0.get_mut(namespace) {
            None => Err(Error::UnknownKey),
            Some(map) => match map.remove(from) {
                Some(value) => {
                    map.insert(to.clone(), value);
                    Ok(())
                }
                None => Err(Error::UnknownKey),
            },
        }
    }

    fn list_keys(&self, namespace: &NamespaceBuf, scope: &Scope) -> Vec<Key> {
        self.0
            .get(namespace)
            .map(|m| {
                m.keys()
                    .filter(|k| k.scope().starts_with(scope))
                    .cloned()
                    .collect::<Vec<Key>>()
            })
            .unwrap_or_default()
    }

    fn list_scopes(&self, namespace: &NamespaceBuf) -> Vec<Scope> {
        let scopes: BTreeSet<Scope> = self
            .0
            .get(namespace)
            .map(|m| m.keys().flat_map(|k| k.scope().sub_scopes()).collect())
            .unwrap_or_default();

        scopes.into_iter().collect()
    }

    fn delete_scope(&mut self, namespace: &NamespaceBuf, scope: &Scope) -> Result<()> {
        if let Some(map) = self.0.get_mut(namespace) {
            map.retain(|k, _| !k.scope().starts_with(scope));
        }

        Ok(())
    }

    fn move_scope(&mut self, namespace: &NamespaceBuf, from: &Scope, to: &Scope) -> Result<()> {
        if let Some(map) = self.0.get_mut(namespace) {
            *map = map
                .drain()
                .map(|(k, v)| {
                    if k.scope() == from {
                        (Key::new_scoped(to.clone(), k.name()), v)
                    } else {
                        (k, v)
                    }
                })
                .collect::<HashMap<Key, serde_json::Value>>();
        }

        Ok(())
    }

    fn migrate_namespace(&mut self, from: &NamespaceBuf, to: &NamespaceBuf) -> Result<()> {
        if !self.namespace_is_empty(to) {
            Err(Error::NamespaceMigration(format!(
                "target in-memory namespace {} is not empty",
                to.as_str()
            )))
        } else {
            match self.0.remove(from) {
                None => Err(Error::NamespaceMigration(format!(
                    "original in-memory namespace {} does not exist",
                    from.as_str()
                ))),
                Some(map) => {
                    self.0.insert(to.clone(), map);
                    Ok(())
                }
            }
        }
    }

    pub fn clear(&mut self, namespace: &NamespaceBuf) -> Result<()> {
        self.0.insert(namespace.clone(), HashMap::new());
        Ok(())
    }
}

lazy_static! {
    static ref STORE: Mutex<MemoryStore> = Mutex::new(MemoryStore::new());
    static ref LOCKS: Mutex<Vec<Scope>> = Mutex::new(Vec::new());
}

#[derive(Debug)]
pub(crate) struct Memory {
    // Used to prevent namespace collisions in the shared (lazy static) in memory structure.
    namespace_prefix: Option<String>,
    effective_namespace: NamespaceBuf,
    inner: &'static Mutex<MemoryStore>,
    locks: &'static Mutex<Vec<Scope>>,
}

impl Memory {
    pub(crate) fn new(namespace_prefix: Option<&str>, namespace: NamespaceBuf) -> Result<Self> {
        let namespace_prefix = namespace_prefix.map(|s| s.to_string());
        let effective_namespace = Self::effective_namespace(&namespace_prefix, namespace)?;

        Ok(Memory {
            namespace_prefix,
            effective_namespace,
            inner: &STORE,
            locks: &LOCKS,
        })
    }

    fn effective_namespace(
        namespace_prefix: &Option<String>,
        namespace: NamespaceBuf,
    ) -> Result<NamespaceBuf> {
        if let Some(pfx) = namespace_prefix {
            NamespaceBuf::from_str(&format!("{}_{}", pfx, namespace)).map_err(|e| {
                Error::UnknownScheme(format!(
                    "cannot parse prefix '{}' for memory store: {}",
                    pfx, e
                ))
            })
        } else {
            Ok(namespace)
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
        write!(f, "KeyValueStore::Memory({})", self.effective_namespace)
    }
}

struct ReadOnlyMemory {
    namespace: NamespaceBuf,
    inner: MemoryStore,
}

impl ReadStore for ReadOnlyMemory {
    fn is_empty(&self) -> Result<bool> {
        // We have one shared inner MemoryStore for all namespaces.
        // In this context this instance is considered empty if the
        // shared store is empty for this namespace.
        Ok(self.inner.namespace_is_empty(&self.namespace))
    }

    fn has(&self, key: &Key) -> Result<bool> {
        Ok(self.inner.has(&self.namespace, key))
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        Ok(self.inner.has_scope(&self.namespace, scope))
    }

    fn get(&self, key: &Key) -> Result<Option<serde_json::Value>> {
        Ok(self.inner.get(&self.namespace, key))
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        Ok(self.inner.list_keys(&self.namespace, scope))
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        Ok(self.inner.list_scopes(&self.namespace))
    }
}

impl KeyValueStoreBackend for Memory {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        // Try to get a lock for 10 seconds. We may need to make this configurable.
        // Dependent on use cases it may actually not be that exceptional for locks
        // to be kept for even longer.
        let wait_ms = 10;
        let tries = 1000;

        for i in 0..tries {
            let mut locks = self
                .locks
                .lock()
                .map_err(|e| Error::MutexLock(e.to_string()))?;

            if locks.iter().any(|s| s.matches(scope)) {
                if i >= tries {
                    return Err(Error::MutexLock(format!("Scope {} already locked", scope)));
                } else {
                    drop(locks);
                    std::thread::sleep(std::time::Duration::from_millis(wait_ms));
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
    fn is_empty(&self) -> Result<bool> {
        self.lock()
            .map(|l| l.namespace_is_empty(&self.effective_namespace))
    }

    fn has(&self, key: &Key) -> Result<bool> {
        Ok(self.lock()?.has(&self.effective_namespace, key))
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        Ok(self.lock()?.has_scope(&self.effective_namespace, scope))
    }

    fn get(&self, key: &Key) -> Result<Option<serde_json::Value>> {
        Ok(self.lock()?.get(&self.effective_namespace, key))
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        Ok(self.lock()?.list_keys(&self.effective_namespace, scope))
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        Ok(self.lock()?.list_scopes(&self.effective_namespace))
    }
}

impl WriteStore for Memory {
    fn store(&self, key: &Key, value: serde_json::Value) -> Result<()> {
        self.lock()?.insert(&self.effective_namespace, key, value);
        Ok(())
    }

    fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        self.lock()?.move_value(&self.effective_namespace, from, to)
    }

    fn delete(&self, key: &Key) -> Result<()> {
        self.lock()?.delete(&self.effective_namespace, key)
    }

    fn delete_scope(&self, scope: &Scope) -> Result<()> {
        self.lock()?.delete_scope(&self.effective_namespace, scope)
    }

    fn clear(&self) -> Result<()> {
        self.lock()?.clear(&self.effective_namespace)
    }

    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        self.lock()?.move_scope(&self.effective_namespace, from, to)
    }

    fn migrate_namespace(&mut self, to: NamespaceBuf) -> Result<()> {
        // We need to preserve the namespace prefix if it was set.
        // This prefix is used to prevent namespace collisions in the
        // shared (lazy static) in memory structure.
        let effective_to = Self::effective_namespace(&self.namespace_prefix, to)?;

        self.lock()?
            .migrate_namespace(&self.effective_namespace, &effective_to)?;
        self.effective_namespace = effective_to;

        Ok(())
    }
}
