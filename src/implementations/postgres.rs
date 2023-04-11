use std::{
    fmt::Debug,
    sync::{Mutex, MutexGuard},
};

use postgres::{NoTls, Row, ToStatement, Transaction};
use postgres_types::ToSql;
use r2d2_postgres::{
    r2d2::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use url::Url;

use crate::{
    key::Segment, Key, KeyValueStoreBackend, ReadStore, Result, Scope, TransactionCallback,
    WriteStore,
};

type PostgresClient = PostgresConnectionManager<NoTls>;

pub type PgPool = Pool<PostgresClient>;

#[derive(Debug)]
pub(crate) struct Postgres<E> {
    namespace: Segment,
    executor: E,
}

impl Postgres<PgPool> {
    pub(crate) fn new(connection_str: &Url, namespace: Segment) -> Result<Self> {
        let manager = PostgresConnectionManager::new(connection_str.as_str().parse()?, NoTls);
        let pool = Pool::new(manager)?;

        Ok(Postgres {
            namespace,
            executor: pool,
        })
    }
}

impl<E> KeyValueStoreBackend for Postgres<E>
where
    E: HasExecutor,
{
    fn transaction(&self, _scope: &Scope, callback: TransactionCallback) -> Result<()> {
        const TRIES: usize = 10;

        for i in 0..=TRIES {
            let mut client = self.executor.executor()?;
            let mut transaction = client.exec_transaction()?;
            transaction.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])?;

            let mut postgres = Postgres {
                namespace: self.namespace.clone(),
                executor: Mutex::new(transaction),
            };

            if let Err(e) = callback(&mut postgres) {
                postgres.executor.into_inner().unwrap().rollback()?;

                if i == TRIES {
                    Err(e)?;
                }
            } else {
                postgres.executor.into_inner().unwrap().commit()?;
                break;
            }
        }

        Ok(())
    }
}

impl<E: HasExecutor> ReadStore for Postgres<E> {
    fn has(&self, key: &Key) -> Result<bool> {
        let key = key.with_namespace(self.namespace.clone());

        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), key.name()],
            )?
            .is_some())
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        let scope = scope.with_namespace(self.namespace.clone());

        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE scope[:$2]  = $1",
                &[scope.as_vec(), &scope.len()],
            )?
            .is_some())
    }

    fn get(&self, key: &Key) -> Result<Option<serde_json::Value>> {
        let key = key.with_namespace(self.namespace.clone());
        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT value FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), key.name()],
            )?
            .and_then(|row| row.get(0)))
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        let scope = scope.with_namespace(self.namespace.clone());
        Ok(self
            .executor
            .executor()?
            .exec_query(
                "SELECT scope, key FROM store WHERE scope[:$2] = $1",
                &[scope.as_vec(), &scope.len()],
            )?
            .into_iter()
            .map(|row| {
                let scope: Vec<Segment> = row.get(0);
                let mut scope = Scope::new(scope);
                scope.remove_namespace(self.namespace.clone());
                let name: Segment = row.get(1);

                Key::new_scoped(scope, name)
            })
            .collect::<Vec<Key>>())
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        Ok(self
            .executor
            .executor()?
            .exec_query("SELECT scope FROM store", &[])?
            .into_iter()
            .flat_map(|row| {
                let scope: Vec<Segment> = row.get(0);
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

impl<E: HasExecutor> WriteStore for Postgres<E> {
    fn store(&self, key: &Key, value: serde_json::Value) -> Result<()> {
        let key = key.with_namespace(self.namespace.clone());
        self.executor.executor()?.exec_execute(
            "INSERT INTO store (scope, key, value) VALUES ($1, $2, $3) ON CONFLICT (scope, key) \
             DO UPDATE SET value = $3",
            &[key.scope().as_vec(), key.name(), &value],
        )?;

        Ok(())
    }

    fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        let from = from.with_namespace(self.namespace.clone());
        let to = to.with_namespace(self.namespace.clone());

        self.executor.executor()?.exec_execute(
            "UPDATE store SET scope = $3, key = $4 WHERE scope = $1 AND key = $2",
            &[
                from.scope().as_vec(),
                from.name(),
                to.scope().as_vec(),
                to.name(),
            ],
        )?;

        Ok(())
    }

    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        let from = from.with_namespace(self.namespace.clone());
        let to = to.with_namespace(self.namespace.clone());

        self.executor.executor()?.exec_execute(
            "UPDATE store SET scope = $2 WHERE scope = $1",
            &[&from.as_vec(), &to.as_vec()],
        )?;

        Ok(())
    }

    fn delete(&self, key: &Key) -> Result<()> {
        let key = key.with_namespace(self.namespace.clone());
        self.executor.executor()?.exec_execute(
            "DELETE FROM store WHERE scope = $1 AND key = $2",
            &[key.scope().as_vec(), key.name()],
        )?;

        Ok(())
    }

    fn delete_scope(&self, scope: &Scope) -> Result<()> {
        let scope = scope.with_namespace(self.namespace.clone());
        self.executor
            .executor()?
            .exec_execute("DELETE FROM store WHERE scope = $1", &[&scope.as_vec()])?;

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.executor
            .executor()?
            .exec_execute("DELETE FROM store WHERE scope[1] = $1", &[&self.namespace])?;

        Ok(())
    }
}

trait HasExecutor {
    type Executor<'a>: Executor
    where
        Self: 'a;

    fn executor(&self) -> Result<Self::Executor<'_>>;
}

impl HasExecutor for PgPool {
    type Executor<'a> = PooledConnection<PostgresClient> where Self: 'a;

    fn executor(&self) -> Result<Self::Executor<'_>> {
        Ok(self.get()?)
    }
}

impl<'b> HasExecutor for Mutex<Transaction<'b>> {
    type Executor<'a> = MutexGuard<'a, Transaction<'b>> where Self: 'a;

    fn executor(&self) -> Result<Self::Executor<'_>> {
        Ok(self.lock().unwrap())
    }
}

pub trait Executor {
    fn exec_transaction(&mut self) -> Result<Transaction<'_>>;

    fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement;

    fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement;

    fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement;
}

impl<E: HasExecutor> Executor for Postgres<E> {
    fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        todo!()
    }

    fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement,
    {
        self.executor.executor()?.exec_query(query, params)
    }

    fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement,
    {
        self.executor.executor()?.exec_query_opt(query, params)
    }

    fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement,
    {
        self.executor.executor()?.exec_execute(query, params)
    }
}

impl Executor for PooledConnection<PostgresClient> {
    fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.transaction()?)
    }

    fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self.query(query, params)?)
    }

    fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self.query_opt(query, params)?)
    }

    fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self.execute(query, params)?)
    }
}

impl Executor for MutexGuard<'_, Transaction<'_>> {
    fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.transaction()?)
    }

    fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self.query(query, params)?)
    }

    fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self.query_opt(query, params)?)
    }

    fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement,
    {
        Ok(self.execute(query, params)?)
    }
}
