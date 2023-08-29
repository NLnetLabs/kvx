pub(crate) mod disk;
pub(crate) mod memory;

#[cfg(feature = "postgres")]
pub(crate) mod postgres;

#[cfg(test)]
mod tests {
    use std::{fs, iter};

    use rand::{distributions::Alphanumeric, Rng};
    use serde_json::Value;

    use super::{disk::Disk, memory::Memory};
    #[cfg(feature = "postgres")]
    use crate::implementations::postgres::{PgPool, Postgres};
    use crate::{Key, KeyValueStoreBackend, NamespaceBuf, Scope, SegmentBuf};

    fn random_value(length: usize) -> Value {
        Value::from(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(length)
                .map(char::from)
                .collect::<String>(),
        )
    }

    fn random_segment() -> SegmentBuf {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect::<String>()
            .parse()
            .unwrap()
    }

    fn random_namespace() -> NamespaceBuf {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect::<String>()
            .parse()
            .unwrap()
    }

    fn random_scope(depth: usize) -> Scope {
        Scope::new(iter::repeat_with(random_segment).take(depth).collect())
    }

    fn random_key(depth: usize) -> Key {
        Key::new_scoped(random_scope(depth), random_segment())
    }

    fn test_store(store: impl KeyValueStoreBackend) {
        let key = random_key(1);
        let value = random_value(8);
        store.store(&key, value.clone()).unwrap();

        let result = store.get(&key).unwrap();

        assert_eq!(result, Some(value));

        store.clear().unwrap();
    }

    fn test_has(store: impl KeyValueStoreBackend) {
        let key = random_key(1);
        let value = random_value(8);
        store.store(&key, value).unwrap();

        assert!(store.has(&key).unwrap());
        assert!(!store.has(&random_key(1)).unwrap());

        store.clear().unwrap();
    }

    fn test_has_scope(store: impl KeyValueStoreBackend) {
        let scope = random_scope(2);
        let key = Key::new_scoped(scope.clone(), random_segment());
        let value = random_value(8);

        store.store(&key, value).unwrap();

        for s in scope.sub_scopes() {
            assert!(store.has_scope(&s).unwrap());
        }
        assert!(!store.has_scope(&random_scope(1)).unwrap());

        store.clear().unwrap();
    }

    fn test_list_keys(store: impl KeyValueStoreBackend) {
        let ns = random_segment();
        let keys: Vec<Key> = vec![
            random_key(1),
            random_key(1).with_super_scope(ns.clone()),
            random_key(1).with_super_scope(ns.clone()),
            random_key(2).with_super_scope(ns.clone()),
        ];

        for key in keys.iter() {
            store.store(key, random_value(8)).unwrap();
        }

        let mut result = store.list_keys(&Scope::from_segment(ns)).unwrap();
        let mut expected: Vec<Key> = keys[1..].to_vec();

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_list_scopes(store: impl KeyValueStoreBackend) {
        let name = random_segment();
        let scope = random_scope(1);
        let scope1 = random_scope(2);
        let scope2 = random_scope(1);
        let value = random_value(8);
        store
            .store(&Key::new_scoped(scope.clone(), name.clone()), value.clone())
            .unwrap();
        store
            .store(
                &Key::new_scoped(scope1.clone(), name.clone()),
                value.clone(),
            )
            .unwrap();
        store
            .store(&Key::new_scoped(scope2.clone(), name), value)
            .unwrap();

        let mut result = store.list_scopes().unwrap();
        let mut expected = [scope.sub_scopes(), scope1.sub_scopes(), scope2.sub_scopes()].concat();

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_move_value(store: impl KeyValueStoreBackend) {
        let from = random_key(1);
        let to = random_key(1);
        let value = random_value(8);

        store.store(&from, value.clone()).unwrap();
        store.move_value(&from, &to).unwrap();

        let result = store.get(&to).unwrap();

        assert_eq!(result, Some(value));

        // test deeper scope
        let from = random_key(3);
        let to = random_key(2);
        let value = random_value(8);

        store.store(&from, value.clone()).unwrap();
        store.move_value(&from, &to).unwrap();

        let result = store.get(&to).unwrap();

        assert_eq!(result, Some(value));

        store.clear().unwrap();
    }

    fn test_delete(store: impl KeyValueStoreBackend) {
        let key = random_key(1);
        store.store(&key, random_value(8)).unwrap();
        store.delete(&key).unwrap();

        let result = store.get(&key).unwrap();

        assert_eq!(result, None);
    }

    fn test_delete_scope(store: impl KeyValueStoreBackend) {
        let key = random_key(0);
        let scope = random_scope(1);
        let scope2 = random_scope(1);
        let keys: Vec<Key> = vec![
            key,
            Key::new_scoped(scope.clone(), random_segment()),
            Key::new_scoped(scope.clone(), random_segment()),
            Key::new_scoped(scope2, random_segment()),
        ];

        for key in keys.iter() {
            store.store(key, random_value(8)).unwrap();
        }

        store.delete_scope(&scope).unwrap();

        let mut result = store.list_keys(&Scope::global()).unwrap();
        let mut expected: Vec<Key> = vec![keys[0].clone(), keys[3].clone()];

        result.sort();
        expected.sort();

        assert_eq!(result, expected);

        store.clear().unwrap();
    }

    fn test_clear(store: impl KeyValueStoreBackend) {
        for i in 1..=4 {
            store.store(&random_key(i), random_value(8)).unwrap();
        }

        store.clear().unwrap();

        let result = store.list_keys(&Scope::global()).unwrap();

        assert_eq!(result, vec![]);

        store.clear().unwrap();
    }

    fn test_is_empty(store: impl KeyValueStoreBackend) {
        assert!(store.is_empty().unwrap());

        store.store(&random_key(1), random_value(8)).unwrap();

        assert!(!store.is_empty().unwrap());
    }

    fn test_move_scope(store: impl KeyValueStoreBackend) {
        let key = random_key(0);
        let scope = random_scope(1);
        let scope2 = random_scope(1);
        let segment = random_segment();
        let segment2 = random_segment();
        let keys: Vec<Key> = vec![
            key.clone(),
            Key::new_scoped(scope.clone(), segment.clone()),
            Key::new_scoped(scope.clone(), segment2.clone()),
            Key::new_scoped(scope2.clone(), segment2.clone()),
        ];

        for key in keys.into_iter() {
            store.store(&key, random_value(8)).unwrap();
        }

        let scope3 = random_scope(1);
        store.move_scope(&scope, &scope3).unwrap();

        let mut result = store.list_keys(&Scope::global()).unwrap();
        let mut expected: Vec<Key> = vec![
            key,
            Key::new_scoped(scope3.clone(), segment),
            Key::new_scoped(scope3, segment2.clone()),
            Key::new_scoped(scope2, segment2),
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

        let transaction_scope = random_scope(1);
        std::thread::scope(|s| {
            let transaction_scope = transaction_scope.clone();
            stores.iter_mut().enumerate().for_each(|(index, store)| {
                let transaction_scope = transaction_scope.clone();
                s.spawn(move || {
                    let mut counter = 0;
                    let counter_ref = &mut counter;

                    let scope_clone = transaction_scope.clone();

                    store
                        .transaction(
                            &transaction_scope.clone(),
                            &mut move |t: &dyn KeyValueStoreBackend| {
                                let current_counter = t.get(&"counter".parse().unwrap())?.unwrap();
                                let c: i32 = serde_json::from_value(current_counter).unwrap();
                                *counter_ref = c;

                                t.store(
                                    &Key::new_scoped(
                                        transaction_scope.clone(),
                                        format!("key_{index}_1_{c}").parse::<SegmentBuf>().unwrap(),
                                    ),
                                    Value::from(format!("value_1_{c}")),
                                )?;
                                t.store(
                                    &Key::new_scoped(
                                        transaction_scope.clone(),
                                        format!("key_{index}_2_{c}").parse::<SegmentBuf>().unwrap(),
                                    ),
                                    Value::from(format!("value_2_{c}")),
                                )?;
                                t.store(
                                    &Key::new_scoped(
                                        transaction_scope.clone(),
                                        format!("key_{index}_3_{c}").parse::<SegmentBuf>().unwrap(),
                                    ),
                                    Value::from(format!("value_3_{c}")),
                                )?;
                                t.store(
                                    &Key::new_scoped(
                                        transaction_scope.clone(),
                                        format!("key_{index}_4_{c}").parse::<SegmentBuf>().unwrap(),
                                    ),
                                    Value::from(format!("value_4_{c}")),
                                )?;

                                t.store(&"counter".parse().unwrap(), Value::from(c + 1))?;

                                Ok(())
                            },
                        )
                        .unwrap();

                    let mut result: Vec<Key> =
                        store.list_keys(&scope_clone).unwrap().into_iter().collect();
                    let expected: Vec<Key> = [
                        format!("key_{index}_1_{counter}"),
                        format!("key_{index}_2_{counter}"),
                        format!("key_{index}_3_{counter}"),
                        format!("key_{index}_4_{counter}"),
                    ]
                    .iter()
                    .map(|s| Key::new_scoped(scope_clone.clone(), s.parse::<SegmentBuf>().unwrap()))
                    .collect();

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

        let result: Vec<Key> = stores[0]
            .list_keys(&Scope::global())
            .unwrap()
            .into_iter()
            .collect();

        let scenario1: Vec<Key> = [
            "key_0_1_0",
            "key_0_2_0",
            "key_0_3_0",
            "key_0_4_0",
            "key_1_1_1",
            "key_1_2_1",
            "key_1_3_1",
            "key_1_4_1",
        ]
        .iter()
        .map(|s| Key::new_scoped(transaction_scope.clone(), s.parse::<SegmentBuf>().unwrap()))
        .collect();

        let scenario2: Vec<Key> = [
            "key_0_1_1",
            "key_0_2_1",
            "key_0_3_1",
            "key_0_4_1",
            "key_1_1_0",
            "key_1_2_0",
            "key_1_3_0",
            "key_1_4_0",
        ]
        .iter()
        .map(|s| Key::new_scoped(transaction_scope.clone(), s.parse::<SegmentBuf>().unwrap()))
        .collect();

        assert!(
            scenario1.iter().all(|k| result.contains(k))
                || scenario2.iter().all(|k| result.contains(k)),
            "invalid scenario: {result:?}"
        );

        // clean up when all threads end
        stores.iter_mut().for_each(|store| {
            store.clear().unwrap();
        });
    }

    macro_rules! generate_tests {
        ($ident:ident, $construct:expr) => {
            mod $ident {
                use serial_test::serial;

                #[test]
                #[serial]
                fn test_store() {
                    super::test_store($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_has() {
                    super::test_has($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_has_scope() {
                    super::test_has_scope($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_list_keys() {
                    super::test_list_keys($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_list_scopes() {
                    super::test_list_scopes($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_move_value() {
                    super::test_move_value($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_delete() {
                    super::test_delete($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_delete_scope() {
                    super::test_delete_scope($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_clear() {
                    super::test_clear($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_is_empty() {
                    super::test_is_empty($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_move_scope() {
                    super::test_move_scope($construct(super::random_namespace()))
                }

                #[test]
                #[serial]
                fn test_transaction() {
                    let ns = super::random_namespace();
                    let store1 = $construct(ns.clone());
                    let store2 = $construct(ns.clone());
                    super::test_transaction(vec![store1, store2]);
                }
            }
        };
    }

    #[cfg(feature = "postgres")]
    fn postgres(namespace: NamespaceBuf) -> Postgres<PgPool> {
        let pg = Postgres::new(
            &url::Url::parse("postgres://postgres@localhost/postgres").unwrap(),
            namespace,
        )
        .unwrap();

        pg.truncate().unwrap();

        pg
    }

    fn memory(namespace: NamespaceBuf) -> Memory {
        use crate::WriteStore;

        let store = Memory::new(namespace);
        store.clear().unwrap();
        store
    }

    fn disk(namespace: NamespaceBuf) -> Disk {
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
    generate_tests!(test_fs, super::disk);
}
