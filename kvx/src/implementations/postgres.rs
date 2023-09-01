use std::{
    cell::{RefCell, RefMut},
    fmt::{Debug, Display},
};

use kvx_types::NamespaceBuf;
use postgres::{NoTls, Row, ToStatement, Transaction};
use postgres_types::ToSql;
use r2d2_postgres::{
    r2d2::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use url::Url;

use crate::{
    Error, Key, KeyValueStoreBackend, ReadStore, Result, Scope, SegmentBuf, TransactionCallback,
    WriteStore,
};

type PostgresClient = PostgresConnectionManager<NoTls>;

pub type PgPool = Pool<PostgresClient>;

#[derive(Debug)]
pub(crate) struct Postgres<E> {
    namespace: NamespaceBuf,
    executor: E,
}

impl Postgres<PgPool> {
    pub(crate) fn new(connection_str: &Url, namespace: impl Into<NamespaceBuf>) -> Result<Self> {
        let manager = PostgresConnectionManager::new(connection_str.as_str().parse()?, NoTls);
        let pool = Pool::new(manager)?;

        Ok(Postgres {
            namespace: namespace.into(),
            executor: pool,
        })
    }

    #[cfg(test)]
    pub(crate) fn truncate(&self) -> Result<()> {
        self.executor
            .executor()?
            .exec_query("TRUNCATE table store", &[])?;

        Ok(())
    }
}

impl<E: HasExecutor> Display for Postgres<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyValueStore::Postgres({})", self.namespace)
    }
}

impl<E: HasExecutor> KeyValueStoreBackend for Postgres<E> {
    fn transaction(&self, _scope: &Scope, callback: TransactionCallback) -> Result<()> {
        const TRIES: usize = 10;

        for i in 0..=TRIES {
            let mut client = self.executor.executor()?;
            let mut transaction = client.exec_transaction()?;
            transaction.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])?;

            let mut postgres = Postgres {
                namespace: self.namespace.clone(),
                executor: RefCell::new(transaction),
            };

            if let Err(e) = callback(&mut postgres) {
                postgres.executor.into_inner().rollback()?;

                if i == TRIES {
                    Err(e)?;
                }
            } else {
                postgres.executor.into_inner().commit()?;
                break;
            }
        }

        Ok(())
    }
}

impl<E: HasExecutor> ReadStore for Postgres<E> {
    fn is_empty(&self) -> Result<bool> {
        // We use a shared table for multiple namespaces. We consider this
        // instance empty if there are no entries for this namespace.
        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE namespace = $1",
                &[&self.namespace],
            )?
            .is_none())
    }

    fn has(&self, key: &Key) -> Result<bool> {
        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE namespace = $1 AND scope = $2 AND key = $3",
                &[&self.namespace, key.scope().as_vec(), &key.name()],
            )?
            .is_some())
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT DISTINCT scope FROM store WHERE namespace = $1 AND scope[:$3]  = $2",
                &[&self.namespace, scope.as_vec(), &scope.len()],
            )?
            .is_some())
    }

    fn get(&self, key: &Key) -> Result<Option<serde_json::Value>> {
        Ok(self
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT value FROM store WHERE namespace = $1 AND scope = $2 AND key = $3",
                &[&self.namespace, key.scope().as_vec(), &key.name()],
            )?
            .and_then(|row| row.get(0)))
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        Ok(self
            .executor
            .executor()?
            .exec_query(
                "SELECT scope, key FROM store WHERE namespace = $1 AND scope[:$3] = $2",
                &[&self.namespace, scope.as_vec(), &scope.len()],
            )?
            .into_iter()
            .map(|row| {
                let scope = Scope::new(row.get(0));
                let name: SegmentBuf = row.get(1);

                Key::new_scoped(scope, name)
            })
            .collect::<Vec<Key>>())
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        Ok(self
            .executor
            .executor()?
            .exec_query(
                "SELECT DISTINCT scope FROM store WHERE namespace = $1",
                &[&self.namespace],
            )?
            .into_iter()
            .flat_map(|row| Scope::new(row.get(0)).sub_scopes())
            .collect::<Vec<Scope>>())
    }
}

impl<E: HasExecutor> WriteStore for Postgres<E> {
    fn store(&self, key: &Key, value: serde_json::Value) -> Result<()> {
        self.executor.executor()?.exec_execute(
            "INSERT INTO store (namespace, scope, key, value) VALUES ($1, $2, $3, $4) ON CONFLICT (namespace, scope, key) \
             DO UPDATE SET value = $4",
            &[&self.namespace, key.scope().as_vec(), &key.name(), &value],
        )?;

        Ok(())
    }

    fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        self.executor.executor()?.exec_execute(
            "UPDATE store SET scope = $4, key = $5 WHERE namespace = $1 AND scope = $2 AND key = $3",
            &[
                &self.namespace,
                from.scope().as_vec(),
                &from.name(),
                to.scope().as_vec(),
                &to.name(),
            ],
        )?;

        Ok(())
    }

    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        self.executor.executor()?.exec_execute(
            "UPDATE store SET scope = $3 WHERE namespace = $1 AND scope = $2",
            &[&self.namespace, &from.as_vec(), &to.as_vec()],
        )?;

        Ok(())
    }

    fn delete(&self, key: &Key) -> Result<()> {
        self.executor.executor()?.exec_execute(
            "DELETE FROM store WHERE namespace = $1 AND scope = $2 AND key = $3",
            &[&self.namespace, key.scope().as_vec(), &key.name()],
        )?;

        Ok(())
    }

    fn delete_scope(&self, scope: &Scope) -> Result<()> {
        self.executor.executor()?.exec_execute(
            "DELETE FROM store WHERE namespace = $1 AND scope = $2",
            &[&self.namespace, &scope.as_vec()],
        )?;

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.executor
            .executor()?
            .exec_execute("DELETE FROM store WHERE namespace = $1", &[&self.namespace])?;

        Ok(())
    }

    fn migrate_namespace(&mut self, to: NamespaceBuf) -> Result<()> {
        let mut client = self.executor.executor()?;
        let mut transaction = client.exec_transaction()?;
        transaction.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])?;

        let postgres = Postgres {
            namespace: self.namespace.clone(),
            executor: RefCell::new(transaction),
        };

        if postgres
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT DISTINCT namespace FROM store WHERE namespace = $1",
                &[&self.namespace],
            )?
            .is_none()
        {
            postgres.executor.into_inner().rollback()?; // make sure transaction is finished

            return Err(Error::NamespaceMigration(format!(
                "original namespace {} not found in database",
                &self.namespace
            )));
        }

        if postgres
            .executor
            .executor()?
            .exec_query_opt(
                "SELECT DISTINCT namespace FROM store WHERE namespace = $1",
                &[&to],
            )?
            .is_some()
        {
            postgres.executor.into_inner().rollback()?; // make sure transaction is finished

            return Err(Error::NamespaceMigration(format!(
                "target namespace {} already exists in database",
                &self.namespace
            )));
        }

        postgres.executor.executor()?.exec_execute(
            "UPDATE store SET namespace = $2 WHERE namespace = $1",
            &[&self.namespace, &to],
        )?;
        postgres.executor.into_inner().commit()?;

        self.namespace = to;

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

impl<'b> HasExecutor for RefCell<Transaction<'b>> {
    type Executor<'a> = RefMut<'a, Transaction<'b>> where Self: 'a;

    fn executor(&self) -> Result<Self::Executor<'_>> {
        Ok(self.borrow_mut())
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

impl Executor for RefMut<'_, Transaction<'_>> {
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
