pub(crate) mod disk;
pub(crate) mod memory;

#[cfg(feature = "postgres")]
pub(crate) mod postgres;

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use rand::{distributions::Alphanumeric, Rng};
    use serde_json::Value;

    use super::{disk::Disk, memory::Memory};
    #[cfg(feature = "postgres")]
    use crate::implementations::postgres::{PgPool, Postgres};
    use crate::{key::Segment, Key, KeyValueStoreBackend, Scope};

    fn test_store(store: impl KeyValueStoreBackend) {
        store
            .store(&"foo".parse().unwrap(), Value::from("bar"))
            .unwrap();

        let result = store.get(&"foo".parse().unwrap()).unwrap();
        let expected = Some(Value::from("bar"));

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_has(store: impl KeyValueStoreBackend) {
        store
            .store(&"foo".parse().unwrap(), Value::from("bar"))
            .unwrap();

        assert!(store.has(&"foo".parse().unwrap()).unwrap());
        assert!(!store.has(&"faa".parse().unwrap()).unwrap());

        store.clear().unwrap();
    }

    fn test_has_scope(store: impl KeyValueStoreBackend) {
        store
            .store(
                &format!("boo{sep}bee{sep}bar", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
                Value::from("bar"),
            )
            .unwrap();

        assert!(store.has_scope(&"boo".parse().unwrap()).unwrap());
        assert!(store
            .has_scope(
                &format!("boo{sep}bee", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap()
            )
            .unwrap());
        assert!(!store.has_scope(&"baa".parse().unwrap()).unwrap());

        store.clear().unwrap();
    }

    fn test_list_keys(store: impl KeyValueStoreBackend) {
        let keys: Vec<Key> = vec![
            "boo".parse().unwrap(),
            format!("fee{sep}bar", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo{sep}mee", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        for key in keys.into_iter() {
            store.store(&key, Value::from("bar")).unwrap();
        }

        let mut result = store.list_keys(&"fee".parse().unwrap()).unwrap();
        let mut expected: Vec<Key> = vec![
            format!("fee{sep}bar", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo{sep}mee", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_list_scopes(store: impl KeyValueStoreBackend) {
        store
            .store(
                &format!("foo{sep}keyname", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
                Value::from("bar"),
            )
            .unwrap();
        store
            .store(
                &format!("boo{sep}bee{sep}keyname", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
                Value::from("bar"),
            )
            .unwrap();
        store
            .store(
                &format!("woo{sep}keyname", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
                Value::from("bar"),
            )
            .unwrap();

        let mut result = store.list_scopes().unwrap();
        let mut expected: Vec<Scope> = vec![
            "foo".parse().unwrap(),
            "boo".parse().unwrap(),
            format!("boo{sep}bee", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            "woo".parse().unwrap(),
        ];

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_move_value(store: impl KeyValueStoreBackend) {
        store
            .store(&"foo".parse().unwrap(), Value::from("bar123"))
            .unwrap();
        store
            .move_value(&"foo".parse().unwrap(), &"fee".parse().unwrap())
            .unwrap();

        let result = store.get(&"fee".parse().unwrap()).unwrap();

        assert_eq!(result, Some(Value::from("bar123")));

        store
            .store(
                &format!("foo{sep}bar", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
                Value::from("bar123"),
            )
            .unwrap();
        store
            .move_value(
                &format!("foo{sep}bar", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
                &format!("fee{sep}bor", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
            )
            .unwrap();

        let result = store
            .get(
                &format!("fee{sep}bor", sep = Scope::SEPARATOR)
                    .parse()
                    .unwrap(),
            )
            .unwrap();

        assert_eq!(result, Some(Value::from("bar123")));

        store.clear().unwrap();
    }

    fn test_delete(store: impl KeyValueStoreBackend) {
        store
            .store(&"foo".parse().unwrap(), Value::from("bar"))
            .unwrap();
        store.delete(&"foo".parse().unwrap()).unwrap();

        let result = store.get(&"foo".parse().unwrap()).unwrap();

        assert_eq!(result, None);
    }

    fn test_delete_scope(store: impl KeyValueStoreBackend) {
        let keys: Vec<Key> = vec![
            "boo".parse().unwrap(),
            format!("mee{sep}bar", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("mee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        for key in keys.into_iter() {
            store.store(&key, Value::from("bar")).unwrap();
        }

        store.delete_scope(&"mee".parse().unwrap()).unwrap();

        let mut result = store.list_keys(&Scope::global()).unwrap();
        let mut expected: Vec<Key> = vec![
            "boo".parse().unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_clear(store: impl KeyValueStoreBackend) {
        let keys: Vec<Key> = vec![
            "boo".parse().unwrap(),
            format!("fee{sep}bar", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo{sep}mee", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        for key in keys.into_iter() {
            store.store(&key, Value::from("bar")).unwrap();
        }

        store.clear().unwrap();

        let result = store.list_keys(&Scope::global()).unwrap();

        assert_eq!(result, vec![]);

        store.clear().unwrap();
    }

    fn test_move_scope(store: impl KeyValueStoreBackend) {
        let keys: Vec<Key> = vec![
            "boo".parse().unwrap(),
            format!("mee{sep}bar", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("mee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        for key in keys.into_iter() {
            store.store(&key, Value::from("bar")).unwrap();
        }

        store
            .move_scope(&"mee".parse().unwrap(), &"boo".parse().unwrap())
            .unwrap();

        let mut result = store.list_keys(&Scope::global()).unwrap();
        let mut expected: Vec<Key> = vec![
            "boo".parse().unwrap(),
            format!("boo{sep}bar", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("boo{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
            format!("fee{sep}foo", sep = Scope::SEPARATOR)
                .parse()
                .unwrap(),
        ];

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_transaction(mut stores: Vec<impl KeyValueStoreBackend + Send>) {
        stores[0]
            .store(&"counter".parse().unwrap(), Value::from(0))
            .unwrap();

        std::thread::scope(|scope| {
            stores.iter_mut().enumerate().for_each(|(index, store)| {
                scope.spawn(move || {
                    let counter = Arc::new(Mutex::new(0));
                    let counter_ref = counter.clone();
                    let key_scope: Scope = "foo".parse().unwrap();

                    store
                        .transaction(
                            &key_scope,
                            Box::new(move |s: &dyn KeyValueStoreBackend| {
                                let current_counter = s.get(&"counter".parse().unwrap())?.unwrap();
                                let mut c = counter_ref.lock().unwrap();
                                *c = serde_json::from_value(current_counter).unwrap();

                                s.store(
                                    &format!("foo{}key_{index}_{c}_1", Scope::SEPARATOR)
                                        .parse()
                                        .unwrap(),
                                    Value::from(format!("value_{c}_1")),
                                )?;
                                s.store(
                                    &format!("foo{}key_{index}_{c}_2", Scope::SEPARATOR)
                                        .parse()
                                        .unwrap(),
                                    Value::from(format!("value_{c}_2")),
                                )?;
                                s.store(
                                    &format!("foo{}key_{index}_{c}_3", Scope::SEPARATOR)
                                        .parse()
                                        .unwrap(),
                                    Value::from(format!("value_{c}_3")),
                                )?;
                                s.store(
                                    &format!("foo{}key_{index}_{c}_4", Scope::SEPARATOR)
                                        .parse()
                                        .unwrap(),
                                    Value::from(format!("value_{c}_4")),
                                )?;

                                s.store(&"counter".parse().unwrap(), Value::from(*c + 1))?;

                                Ok(())
                            }),
                        )
                        .unwrap();

                    let c = counter.lock().unwrap();

                    let mut result = store
                        .list_keys(&key_scope)
                        .unwrap()
                        .into_iter()
                        .map(|k: Key| k.to_string())
                        .collect::<Vec<_>>();
                    let expected: Vec<String> = vec![
                        format!("foo{}key_{index}_{c}_1", Scope::SEPARATOR),
                        format!("foo{}key_{index}_{c}_2", Scope::SEPARATOR),
                        format!("foo{}key_{index}_{c}_3", Scope::SEPARATOR),
                        format!("foo{}key_{index}_{c}_4", Scope::SEPARATOR),
                    ];

                    drop(c);

                    result.sort();

                    for key in expected {
                        assert!(
                            result.contains(&key),
                            "key {key} does not exist in {result:?}"
                        );
                    }
                });
            });
        });

        let key_scope = "foo".parse().unwrap();
        let result = stores[0]
            .list_keys(&key_scope)
            .unwrap()
            .into_iter()
            .map(|k: Key| k.to_string())
            .collect::<Vec<_>>();

        let scenario_1: Vec<String> = vec![
            format!("foo{}key_0_0_1", Scope::SEPARATOR),
            format!("foo{}key_0_0_2", Scope::SEPARATOR),
            format!("foo{}key_0_0_3", Scope::SEPARATOR),
            format!("foo{}key_0_0_4", Scope::SEPARATOR),
            format!("foo{}key_1_1_1", Scope::SEPARATOR),
            format!("foo{}key_1_1_2", Scope::SEPARATOR),
            format!("foo{}key_1_1_3", Scope::SEPARATOR),
            format!("foo{}key_1_1_4", Scope::SEPARATOR),
        ];

        let scenario_2: Vec<String> = vec![
            format!("foo{}key_0_1_1", Scope::SEPARATOR),
            format!("foo{}key_0_1_2", Scope::SEPARATOR),
            format!("foo{}key_0_1_3", Scope::SEPARATOR),
            format!("foo{}key_0_1_4", Scope::SEPARATOR),
            format!("foo{}key_1_0_1", Scope::SEPARATOR),
            format!("foo{}key_1_0_2", Scope::SEPARATOR),
            format!("foo{}key_1_0_3", Scope::SEPARATOR),
            format!("foo{}key_1_0_4", Scope::SEPARATOR),
        ];

        assert!(
            scenario_1.iter().all(|k| result.contains(k))
                || scenario_2.iter().all(|k| result.contains(k)),
            "Invalid scenario: {result:?}"
        );

        // clean up when all threads end
        stores.iter_mut().for_each(|store| {
            store.clear().unwrap();
        });
    }

    fn random_ns() -> Segment {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect::<String>()
            .parse()
            .unwrap()
    }

    macro_rules! generate_tests {
        ($ident:ident, $construct:expr) => {
            mod $ident {
                use serial_test::serial;

                #[test]
                #[serial]
                fn test_store() {
                    super::test_store($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_has() {
                    super::test_has($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_has_scope() {
                    super::test_has_scope($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_list_keys() {
                    super::test_list_keys($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_list_scopes() {
                    super::test_list_scopes($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_move_value() {
                    super::test_move_value($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_delete() {
                    super::test_delete($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_delete_scope() {
                    super::test_delete_scope($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_clear() {
                    super::test_clear($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_move_scope() {
                    super::test_move_scope($construct(super::random_ns()))
                }

                #[test]
                #[serial]
                fn test_transaction() {
                    let ns = super::random_ns();
                    let store1 = $construct(ns.clone());
                    let store2 = $construct(ns.clone());
                    super::test_transaction(vec![store1, store2]);
                }
            }
        };
    }

    #[cfg(feature = "postgres")]
    fn postgres(namespace: Segment) -> Postgres<PgPool> {
        let pg = Postgres::new(
            &url::Url::parse("postgres://postgres@localhost/postgres").unwrap(),
            namespace,
        )
        .unwrap();

        pg.truncate().unwrap();

        pg
    }

    fn memory(namespace: Segment) -> Memory {
        let store = Memory::new(namespace);
        store.lock().unwrap().clear();
        store
    }

    fn fs(namespace: Segment) -> Disk {
        let cwd = std::env::current_dir().unwrap().join("data");
        let path = cwd.to_str().unwrap();
        if cwd.exists() {
            fs::remove_dir_all(path).unwrap();
        }

        Disk::new(path, namespace.as_str()).unwrap()
    }

    #[cfg(feature = "postgres")]
    generate_tests!(test_postgres, super::postgres);
    generate_tests!(test_memory, super::memory);
    generate_tests!(test_fs, super::fs);
}
