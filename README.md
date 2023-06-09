# Key-value store X

Abstraction layer over various key-value store backends in Rust. Tailored to fit the use-cases for [Krill](https://github.com/NLnetLabs/krill).

Switching between backends should be as simple as changing a configuration value.

For now an in-memory, filesystem and Postgres implementation are provided by default.

## Usage

Create an instance of a KVX store and specify the storage backend using an URL. For example:

```rust
let namespace = Segment::parse("some-namespace")?;

// in memory backend
let store = KeyValueStore::new(&Url::parse("memory://")?, namespace)?;

// use a file backend
let store = KeyValueStore::new(&Url::parse("local://tmp")?, namespace)?;

// use a postgres backend
let store = KeyValueStore::new(&Url::parse("postgres://user:password@host/database-name")?, namespace)?;
```

A store can be scoped using a namespace. A namespaces can be further divided up in (possibly nested) scopes.

Note that keys, scopes and namespaces have the `Segment` type, this is necessary to encode namespaces, scopes and keys to the filesystem.

The store supports basic key-value operations:

```rust
fn has(&self, key: &Key) -> Result<bool>;
fn has_scope(&self, scope: &Scope) -> Result<bool>;
fn get(&self, key: &Key) -> Result<Option<Value>>;
fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>>;
fn list_scopes(&self) -> Result<Vec<Scope>>;

fn store(&self, key: &Key, value: Value) -> Result<()>;
fn move_value(&self, from: &Key, to: &Key) -> Result<()>;
fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()>;

fn delete(&self, key: &Key) -> Result<()>;
fn delete_scope(&self, scope: &Scope) -> Result<()>;
fn clear(&self) -> Result<()>;
```

Transactions can be used to atomically perform a sequence of operations:

```rust
store.transaction(scope, &mut move |t: &dyn KeyValueStoreBackend| { 
    let key = "counter".parse()?;
    let value = t.get(&key)?;
    let new_value = value.as_i64().unwrap_or_default() + 1;
    t.store(&key, Value::from(new_value))?;
})?;
```

A queue mechanism enables creating and handling tasks. A job can be scheduled at a certain time.

```rust
/// create a new job, optionally at a certain time
fn schedule_job(
    &self,
    name: SegmentBuf,
    value: serde_json::Value,
    timestamp: Option<u64>,
) -> Result<()>;
/// check the number of unclaimed jobs
fn jobs_remaining(&self) -> Result<usize>;
/// check a certain job exists, return the scheduled timestamp
fn exists(&self, name: SegmentBuf) -> Option<u64>;
/// claim an available job (this method checks the scheduled timestamp of jobs)
fn claim_job(&self) -> Option<Task>;
/// mark job as finished (done by the runner)
fn finished_job(&self, task: Task) -> Result<()>;
/// cleanup finished jobs and reschedule timed-out jobs
/// (by default job are rescheduled after 15 minutes of inactivity, finished jobs are removed after 7 days by default)
fn cleanup(
    &self,
    reschedule_after: Option<&Duration>,
    remove_after: Option<&Duration>,
) -> Result<()>;
```

For example:

```rust
use kvx::Queue;

store.schedule_job(key, value, None).unwrap();

assert_eq!(queue.jobs_remaining().unwrap(), 1);

let job = queue.claim_job();

assert!(job.is_some());
assert_eq!(queue.jobs_remaining().unwrap(), 0);
```

## Changelog

### Version 0.6.0

Breaking changes:
- Implicit .json extension for keys on disk were removed (see PR #32)

## Development

In order to make development easy, a `docker-compose.yml` that starts a Postgres container is included. One can start it with:
```
docker compose up
```

When the container is running, one can run tests with:
```
cargo test
```

To run test without including Postgres, run:
```
cargo test --no-default-features
```
