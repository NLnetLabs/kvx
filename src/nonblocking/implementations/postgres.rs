use std::fmt::Debug;

use async_trait::async_trait;
use postgres::{NoTls, Row, ToStatement, Transaction};
use postgres_types::ToSql;
use r2d2_postgres::{
    r2d2::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use url::Url;

use crate::{
    Key, KeyValueStoreBackend, ReadStore, Result, Scope, SegmentBuf, TransactionCallback,
    WriteStore,
};

type PostgresClient = PostgresConnectionManager<NoTls>;

pub type PgPool = Pool<PostgresClient>;

#[derive(Debug)]
pub struct Postgres<E> {
    namespace: SegmentBuf,
    executor: E,
}

impl Postgres<PgPool> {
    pub fn new(connection_str: &Url, namespace: impl Into<SegmentBuf>) -> Result<Self> {
        let manager = PostgresConnectionManager::new(connection_str.as_str().parse()?, NoTls);
        let pool = Pool::new(manager)?;

        Ok(Postgres {
            namespace: namespace.into(),
            executor: pool,
        })
    }
}

#[async_trait]
impl<E> KeyValueStoreBackend for Postgres<E>
where
    E: HasExecutor + Send,
    for<'a> E::Executor<'a>: Send,
{
    async fn transaction(&mut self, _scope: &Scope, callback: TransactionCallback) -> Result<()> {
        todo!();
    }
}

#[async_trait]
impl<E> ReadStore for Postgres<E>
where
    E: HasExecutor + Send,
    for<'a> E::Executor<'a>: Send,
{
    async fn has(&mut self, key: &Key) -> Result<bool> {
        let key = key.with_namespace(self.namespace.clone());

        Ok(self
            .executor
            .get_executor()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), &key.name()],
            )
            .await?
            .is_some())
    }

    async fn has_scope(&mut self, scope: &Scope) -> Result<bool> {
        let scope = scope.with_namespace(self.namespace.clone());

        Ok(self
            .executor
            .get_executor()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE scope[:$2]  = $1",
                &[scope.as_vec(), &scope.len()],
            )
            .await?
            .is_some())
    }

    async fn get(&mut self, key: &Key) -> Result<Option<serde_json::Value>> {
        let key = key.with_namespace(self.namespace.clone());
        Ok(self
            .executor
            .get_executor()?
            .exec_query_opt(
                "SELECT value FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), &key.name()],
            )
            .await?
            .and_then(|row| row.get(0)))
    }

    async fn list_keys(&mut self, scope: &Scope) -> Result<Vec<Key>> {
        let scope = scope.with_namespace(self.namespace.clone());
        Ok(self
            .executor
            .get_executor()?
            .exec_query(
                "SELECT scope, key FROM store WHERE scope[:$2] = $1",
                &[scope.as_vec(), &scope.len()],
            )
            .await?
            .into_iter()
            .map(|row| {
                let scope: Vec<SegmentBuf> = row.get(0);
                let mut scope = Scope::new(scope);
                scope.remove_namespace(self.namespace.clone());
                let name: SegmentBuf = row.get(1);

                Key::new_scoped(scope, name)
            })
            .collect::<Vec<Key>>())
    }

    async fn list_scopes(&mut self) -> Result<Vec<Scope>> {
        Ok(self
            .executor
            .get_executor()?
            .exec_query("SELECT scope FROM store", &[])
            .await?
            .into_iter()
            .flat_map(|row| {
                let scope: Vec<SegmentBuf> = row.get(0);
                let mut scope = Scope::new(scope);
                if scope.remove_namespace(self.namespace.clone()).is_some() {
                    scope.sub_scopes()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Scope>>())
    }
}

#[async_trait]
impl<E> WriteStore for Postgres<E>
where
    E: HasExecutor + Send,
    for<'a> E::Executor<'a>: Send,
{
    async fn store(&mut self, key: &Key, value: serde_json::Value) -> Result<()> {
        let key = key.with_namespace(self.namespace.clone());
        self.executor
            .get_executor()?
            .exec_execute(
                "INSERT INTO store (scope, key, value) VALUES ($1, $2, $3) ON CONFLICT (scope, \
                 key) DO UPDATE SET value = $3",
                &[key.scope().as_vec(), &key.name(), &value],
            )
            .await?;

        Ok(())
    }

    async fn move_value(&mut self, from: &Key, to: &Key) -> Result<()> {
        let from = from.with_namespace(self.namespace.clone());
        let to = to.with_namespace(self.namespace.clone());

        self.executor
            .get_executor()?
            .exec_execute(
                "UPDATE store SET scope = $3, key = $4 WHERE scope = $1 AND key = $2",
                &[
                    from.scope().as_vec(),
                    &from.name(),
                    to.scope().as_vec(),
                    &to.name(),
                ],
            )
            .await?;

        Ok(())
    }

    async fn move_scope(&mut self, from: &Scope, to: &Scope) -> Result<()> {
        let from = from.with_namespace(self.namespace.clone());
        let to = to.with_namespace(self.namespace.clone());

        self.executor
            .get_executor()?
            .exec_execute(
                "UPDATE store SET scope = $2 WHERE scope = $1",
                &[&from.as_vec(), &to.as_vec()],
            )
            .await?;

        Ok(())
    }

    async fn delete(&mut self, key: &Key) -> Result<()> {
        let key = key.with_namespace(self.namespace.clone());
        self.executor
            .get_executor()?
            .exec_execute(
                "DELETE FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), &key.name()],
            )
            .await?;

        Ok(())
    }

    async fn delete_scope(&mut self, scope: &Scope) -> Result<()> {
        let scope = scope.with_namespace(self.namespace.clone());
        self.executor
            .get_executor()?
            .exec_execute("DELETE FROM store WHERE scope = $1", &[&scope.as_vec()])
            .await?;

        Ok(())
    }

    async fn clear(&mut self) -> Result<()> {
        self.executor
            .get_executor()?
            .exec_execute("DELETE FROM store WHERE scope[1] = $1", &[&self.namespace])
            .await?;

        Ok(())
    }
}

trait HasExecutor {
    type Executor<'a>: Executor
    where
        Self: 'a;

    fn get_executor(&mut self) -> Result<Self::Executor<'_>>;
}

impl HasExecutor for PgPool {
    type Executor<'a> = PooledConnection<PostgresClient> where Self: 'a;

    fn get_executor(&mut self) -> Result<Self::Executor<'_>> {
        Ok(self.get()?)
    }
}

impl HasExecutor for Transaction<'_> {
    type Executor<'a> = &'a mut Self where Self: 'a;

    fn get_executor(&mut self) -> Result<Self::Executor<'_>> {
        Ok(self)
    }
}

#[async_trait]
pub trait Executor {
    async fn exec_transaction(&mut self) -> Result<Transaction<'_>>;

    async fn exec_query<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement + Sync;

    async fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement + Sync;

    async fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement + Sync;
}

#[async_trait]
impl<E> Executor for Postgres<E>
where
    E: HasExecutor + Send,
    for<'a> E::Executor<'a>: Send,
{
    async fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        todo!()
    }

    async fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement + Sync,
    {
        self.executor
            .get_executor()?
            .exec_query(query, params)
            .await
    }

    async fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement + Sync,
    {
        self.executor
            .get_executor()?
            .exec_query_opt(query, params)
            .await
    }

    async fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement + Sync,
    {
        self.executor
            .get_executor()?
            .exec_execute(query, params)
            .await
    }
}

#[async_trait]
impl Executor for PooledConnection<PostgresClient> {
    async fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.transaction()?)
    }

    async fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement + Sync,
    {
        Ok(self.query(query, params)?)
    }

    async fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement + Sync,
    {
        Ok(self.query_opt(query, params)?)
    }

    async fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement + Sync,
    {
        Ok(self.execute(query, params)?)
    }
}

#[async_trait]
impl Executor for &mut Transaction<'_> {
    async fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.transaction()?)
    }

    async fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement + Sync,
    {
        Ok(self.query(query, params)?)
    }

    async fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement + Sync,
    {
        Ok(self.query_opt(query, params)?)
    }

    async fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement + Sync,
    {
        Ok(self.execute(query, params)?)
    }
}
