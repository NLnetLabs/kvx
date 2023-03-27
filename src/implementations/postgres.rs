use std::fmt::Debug;

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
    E: GetClient,
{
    fn transaction(&mut self, _scope: &Scope, callback: TransactionCallback) -> Result<()> {
        let tries = 10;

        for i in 0..=tries {
            let mut client = self.executor.get_client()?;
            let mut transaction = client.exec_transaction()?;
            transaction.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])?;

            let mut postgres = Postgres {
                namespace: self.namespace.clone(),
                executor: transaction,
            };

            if let Err(e) = callback(&mut postgres) {
                postgres.executor.rollback()?;

                if i == tries {
                    Err(e)?;
                }
            } else {
                postgres.executor.commit()?;
                break;
            }
        }

        Ok(())
    }
}

impl<E: GetClient> ReadStore for Postgres<E> {
    fn has(&mut self, key: &Key) -> Result<bool> {
        let key = key.with_namespace(self.namespace.clone());

        Ok(self
            .executor
            .get_client()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), key.name()],
            )?
            .is_some())
    }

    fn has_scope(&mut self, scope: &Scope) -> Result<bool> {
        let scope = scope.with_namespace(self.namespace.clone());

        Ok(self
            .executor
            .get_client()?
            .exec_query_opt(
                "SELECT 1 FROM store WHERE scope[:$2]  = $1",
                &[scope.as_vec(), &scope.len()],
            )?
            .is_some())
    }

    fn get(&mut self, key: &Key) -> Result<Option<serde_json::Value>> {
        let key = key.with_namespace(self.namespace.clone());
        Ok(self
            .executor
            .get_client()?
            .exec_query_opt(
                "SELECT value FROM store WHERE scope = $1 AND key = $2",
                &[key.scope().as_vec(), key.name()],
            )?
            .and_then(|row| row.get(0)))
    }

    fn list_keys(&mut self, scope: &Scope) -> Result<Vec<Key>> {
        let scope = scope.with_namespace(self.namespace.clone());
        Ok(self
            .executor
            .get_client()?
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

    fn list_scopes(&mut self) -> Result<Vec<Scope>> {
        Ok(self
            .executor
            .get_client()?
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

impl<E: GetClient> WriteStore for Postgres<E> {
    fn store(&mut self, key: &Key, value: serde_json::Value) -> Result<()> {
        let key = key.with_namespace(self.namespace.clone());
        self.executor.get_client()?.exec_execute(
            "INSERT INTO store (scope, key, value) VALUES ($1, $2, $3) ON CONFLICT (scope, key) \
             DO UPDATE SET value = $3",
            &[key.scope().as_vec(), key.name(), &value],
        )?;

        Ok(())
    }

    fn move_value(&mut self, from: &Key, to: &Key) -> Result<()> {
        let from = from.with_namespace(self.namespace.clone());
        let to = to.with_namespace(self.namespace.clone());

        self.executor.get_client()?.exec_execute(
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

    fn move_scope(&mut self, from: &Scope, to: &Scope) -> Result<()> {
        let from = from.with_namespace(self.namespace.clone());
        let to = to.with_namespace(self.namespace.clone());

        self.executor.get_client()?.exec_execute(
            "UPDATE store SET scope = $2 WHERE scope = $1",
            &[&from.as_vec(), &to.as_vec()],
        )?;

        Ok(())
    }

    fn delete(&mut self, key: &Key) -> Result<()> {
        let key = key.with_namespace(self.namespace.clone());
        self.executor.get_client()?.exec_execute(
            "DELETE FROM store WHERE scope = $1 AND key = $2",
            &[key.scope().as_vec(), key.name()],
        )?;

        Ok(())
    }

    fn delete_scope(&mut self, scope: &Scope) -> Result<()> {
        let scope = scope.with_namespace(self.namespace.clone());
        self.executor
            .get_client()?
            .exec_execute("DELETE FROM store WHERE scope = $1", &[&scope.as_vec()])?;

        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        self.executor
            .get_client()?
            .exec_execute("DELETE FROM store WHERE scope[1] = $1", &[&self.namespace])?;

        Ok(())
    }
}

trait GetClient {
    type Client<'a>: Executor
    where
        Self: 'a;

    fn get_client(&mut self) -> Result<Self::Client<'_>>;
}

impl GetClient for PgPool {
    type Client<'a> = PooledConnection<PostgresClient> where Self: 'a;

    fn get_client(&mut self) -> Result<Self::Client<'_>> {
        Ok(self.get()?)
    }
}

impl GetClient for Transaction<'_> {
    type Client<'a> = &'a mut Self where Self: 'a;

    fn get_client(&mut self) -> Result<Self::Client<'_>> {
        Ok(self)
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

impl<E: GetClient> Executor for Postgres<E> {
    fn exec_transaction(&mut self) -> Result<Transaction<'_>> {
        todo!()
    }

    fn exec_query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement,
    {
        self.executor.get_client()?.exec_query(query, params)
    }

    fn exec_query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement,
    {
        self.executor.get_client()?.exec_query_opt(query, params)
    }

    fn exec_execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement,
    {
        self.executor.get_client()?.exec_execute(query, params)
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

impl Executor for &mut Transaction<'_> {
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
