#![cfg(feature = "async")]

use std::{iter, sync::Arc};

#[cfg(feature = "postgres")]
use kvx::postgres::{PgPool, Postgres};
use kvx::{Key, KeyValueStoreBackend, Scope, SegmentBuf};
use rand::{distributions::Alphanumeric, Rng};
use serde_json::Value;
use tokio::{sync::Mutex, task::JoinSet};

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

fn random_scope(depth: usize) -> Scope {
    Scope::new(iter::repeat_with(random_segment).take(depth).collect())
}

fn random_key(depth: usize) -> Key {
    Key::new_scoped(random_scope(depth), random_segment())
}

async fn test_store(store: impl KeyValueStoreBackend) {
    let key = random_key(1);
    let value = random_value(8);
    store.store(&key, value.clone()).await.unwrap();

    let result = store.get(&key).await.unwrap();

    assert_eq!(result, Some(value));

    store.clear().await.unwrap();
}

async fn test_has(store: impl KeyValueStoreBackend) {
    let key = random_key(1);
    let value = random_value(8);
    store.store(&key, value).await.unwrap();

    assert!(store.has(&key).await.unwrap());
    assert!(!store.has(&random_key(1)).await.unwrap());

    store.clear().await.unwrap();
}

async fn test_has_scope(store: impl KeyValueStoreBackend) {
    let scope = random_scope(2);
    let key = Key::new_scoped(scope.clone(), random_segment());
    let value = random_value(8);

    store.store(&key, value).await.unwrap();

    for s in scope.sub_scopes() {
        assert!(store.has_scope(&s).await.unwrap());
    }
    assert!(!store.has_scope(&random_scope(1)).await.unwrap());

    store.clear().await.unwrap();
}

async fn test_list_keys(store: impl KeyValueStoreBackend) {
    let ns = random_segment();
    let keys: Vec<Key> = vec![
        random_key(1),
        random_key(1).with_namespace(ns.clone()),
        random_key(1).with_namespace(ns.clone()),
        random_key(2).with_namespace(ns.clone()),
    ];

    for key in keys.iter() {
        store.store(key, random_value(8)).await.unwrap();
    }

    let mut result = store.list_keys(&Scope::from_segment(ns)).await.unwrap();
    let mut expected: Vec<Key> = keys[1..].to_vec();

    result.sort();
    expected.sort();

    assert_eq!(result, expected);

    store.clear().await.unwrap();
}

async fn test_list_scopes(store: impl KeyValueStoreBackend) {
    let name = random_segment();
    let scope = random_scope(1);
    let scope1 = random_scope(2);
    let scope2 = random_scope(1);
    let value = random_value(8);
    store
        .store(&Key::new_scoped(scope.clone(), name.clone()), value.clone())
        .await
        .unwrap();
    store
        .store(
            &Key::new_scoped(scope1.clone(), name.clone()),
            value.clone(),
        )
        .await
        .unwrap();
    store
        .store(&Key::new_scoped(scope2.clone(), name), value)
        .await
        .unwrap();

    let mut result = store.list_scopes().await.unwrap();
    let mut expected = vec![scope.sub_scopes(), scope1.sub_scopes(), scope2.sub_scopes()].concat();

    result.sort();
    expected.sort();

    assert_eq!(result, expected);

    store.clear().await.unwrap();
}

async fn test_move_value(store: impl KeyValueStoreBackend) {
    let from = random_key(1);
    let to = random_key(1);
    let value = random_value(8);

    store.store(&from, value.clone()).await.unwrap();
    store.move_value(&from, &to).await.unwrap();

    let result = store.get(&to).await.unwrap();

    assert_eq!(result, Some(value));

    // test deeper scope
    let from = random_key(3);
    let to = random_key(2);
    let value = random_value(8);

    store.store(&from, value.clone()).await.unwrap();
    store.move_value(&from, &to).await.unwrap();

    let result = store.get(&to).await.unwrap();

    assert_eq!(result, Some(value));

    store.clear().await.unwrap();
}

async fn test_delete(store: impl KeyValueStoreBackend) {
    let key = random_key(1);
    store.store(&key, random_value(8)).await.unwrap();
    store.delete(&key).await.unwrap();

    let result = store.get(&key).await.unwrap();

    assert_eq!(result, None);
}

async fn test_delete_scope(store: impl KeyValueStoreBackend) {
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
        store.store(key, random_value(8)).await.unwrap();
    }

    store.delete_scope(&scope).await.unwrap();

    let mut result = store.list_keys(&Scope::global()).await.unwrap();
    let mut expected: Vec<Key> = vec![keys[0].clone(), keys[3].clone()];

    result.sort();
    expected.sort();

    assert_eq!(result, expected);

    store.clear().await.unwrap();
}

async fn test_clear(store: impl KeyValueStoreBackend) {
    for i in 1..=4 {
        store.store(&random_key(i), random_value(8)).await.unwrap();
    }

    store.clear().await.unwrap();

    let result = store.list_keys(&Scope::global()).await.unwrap();

    assert_eq!(result, vec![]);

    store.clear().await.unwrap();
}

async fn test_move_scope(store: impl KeyValueStoreBackend) {
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
        store.store(&key, random_value(8)).await.unwrap();
    }

    let scope3 = random_scope(1);
    store.move_scope(&scope, &scope3).await.unwrap();

    let mut result = store.list_keys(&Scope::global()).await.unwrap();
    let mut expected: Vec<Key> = vec![
        key,
        Key::new_scoped(scope3.clone(), segment),
        Key::new_scoped(scope3, segment2.clone()),
        Key::new_scoped(scope2, segment2),
    ];

    result.sort();
    expected.sort();

    assert_eq!(result, expected);

    store.clear().await.unwrap();
}

async fn test_transaction(mut stores: Vec<impl KeyValueStoreBackend + Send + Sync>) {
    stores[0]
        .store(&"counter".parse().unwrap(), Value::from(0))
        .await
        .unwrap();

    let transaction_scope = random_scope(1);
    let transaction_scope = transaction_scope.clone();
    let mut futures = JoinSet::new();
    for (index, store) in stores.iter_mut().enumerate() {
        let transaction_scope = transaction_scope.clone();
        futures.spawn(async {
            let counter = Arc::new(Mutex::new(0));
            let counter_ref = counter.clone();

            let scope_clone = transaction_scope.clone();

            store
                .transaction(
                    &transaction_scope.clone(),
                    Box::new(|t: &dyn KeyValueStoreBackend| {
                        Box::new(async move {
                            let current_counter =
                                t.get(&"counter".parse().unwrap()).await?.unwrap();
                            let mut c = counter_ref.lock().await;
                            *c = serde_json::from_value(current_counter).unwrap();

                            t.store(
                                &Key::new_scoped(
                                    transaction_scope.clone(),
                                    format!("key_{index}_1_{c}").parse::<SegmentBuf>().unwrap(),
                                ),
                                Value::from(format!("value_1_{c}")),
                            )
                            .await?;
                            t.store(
                                &Key::new_scoped(
                                    transaction_scope.clone(),
                                    format!("key_{index}_2_{c}").parse::<SegmentBuf>().unwrap(),
                                ),
                                Value::from(format!("value_2_{c}")),
                            )
                            .await?;
                            t.store(
                                &Key::new_scoped(
                                    transaction_scope.clone(),
                                    format!("key_{index}_3_{c}").parse::<SegmentBuf>().unwrap(),
                                ),
                                Value::from(format!("value_3_{c}")),
                            )
                            .await?;
                            t.store(
                                &Key::new_scoped(
                                    transaction_scope.clone(),
                                    format!("key_{index}_4_{c}").parse::<SegmentBuf>().unwrap(),
                                ),
                                Value::from(format!("value_4_{c}")),
                            )
                            .await?;

                            t.store(&"counter".parse().unwrap(), Value::from(*c + 1))
                                .await?;

                            Ok(())
                        })
                    }),
                )
                .await
                .unwrap();

            let c = counter.lock().await;

            let mut result: Vec<Key> = store
                .list_keys(&scope_clone)
                .await
                .unwrap()
                .into_iter()
                .collect();
            let expected: Vec<Key> = vec![
                format!("key_{index}_1_{c}"),
                format!("key_{index}_2_{c}"),
                format!("key_{index}_3_{c}"),
                format!("key_{index}_4_{c}"),
            ]
            .iter()
            .map(|s| Key::new_scoped(scope_clone.clone(), s.parse::<SegmentBuf>().unwrap()))
            .collect();

            drop(c);

            result.sort();

            for key in expected {
                assert!(
                    result.contains(&key),
                    "key {key} does not exist in {result:?}"
                );
            }
        });
    }
    while let Some(result) = futures.join_next().await {
        result.unwrap();
    }

    let result: Vec<Key> = stores[0]
        .list_keys(&Scope::global())
        .await
        .unwrap()
        .into_iter()
        .collect();

    let scenario1: Vec<Key> = vec![
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

    let scenario2: Vec<Key> = vec![
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
    for store in &mut stores {
        store.clear().await.unwrap();
    }
}

macro_rules! generate_tests {
    ($ident:ident, $construct:expr) => {
        mod $ident {
            use serial_test::serial;

            #[tokio::test]
            #[serial]
            async fn test_store() {
                super::test_store($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_has() {
                super::test_has($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_has_scope() {
                super::test_has_scope($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_list_keys() {
                super::test_list_keys($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_list_scopes() {
                super::test_list_scopes($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_move_value() {
                super::test_move_value($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_delete() {
                super::test_delete($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_delete_scope() {
                super::test_delete_scope($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_clear() {
                super::test_clear($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_move_scope() {
                super::test_move_scope($construct(super::random_segment()).await).await;
            }

            #[tokio::test]
            #[serial]
            async fn test_transaction() {
                let ns = super::random_segment();
                let store1 = $construct(ns.clone()).await;
                let store2 = $construct(ns.clone()).await;
                super::test_transaction(vec![store1, store2]).await;
            }
        }
    };
}

#[cfg(feature = "postgres")]
async fn postgres(namespace: SegmentBuf) -> Postgres<PgPool> {
    let pg = Postgres::new(
        &url::Url::parse("postgres://postgres@localhost/postgres").unwrap(),
        namespace,
    )
    .await
    .unwrap();

    pg.truncate().await.unwrap();

    pg
}

#[cfg(feature = "postgres")]
generate_tests!(test_postgres, super::postgres);
