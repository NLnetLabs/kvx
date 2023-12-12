# Key-value store X

Abstraction layer over various key-value store backends in Rust. Tailored to fit the use-cases for [Krill](https://github.com/NLnetLabs/krill).

Switching between backends should be as simple as changing a configuration value.

For now an in-memory, filesystem and Postgres implementation are provided by default.

## Usage

Create an instance of a KVX store and specify the storage backend using an URL. For example:

```rust
let namespace = Namespace::parse("some-namespace")?;

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
fn is_empty(&self) -> Result<bool>;
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

/// Migrate the namespace (and all key value pairs) for this store.
fn migrate_namespace(&mut self, to: NamespaceBuf) -> Result<()>;
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

If a value (or a Result for that matter) needs to be returned from within
a transaction, then the execute function can be used. The value can be
a result type in case non kvx errors need to be returned.

Example code where self has a KeyValueStore and wants to return all
keys in the global scope, but also verify that some reserved key is
not used.

The main takeaway being that the closure that is passed in to execute
can return something like `Result<Result<T, E>, kvx::Error>`.

```rust
pub fn list_verified_keys(&self) -> Result<Vec<Keys>, MyError> {
        self.store.execute(&Scope::global(), |kv| {
            let keys = kv.list_keys(&Scope::global())?;
            let forbidden_key = Key::new_global(segment!("reserved"));
            if keys.contains(&forbidden_key) {
                Ok(Err(MyError::ForbiddenKey))
            } else {
                Ok(Ok(keys))
            }
        })
        .map_err(MyError::from)
    }
```

A queue mechanism enables creating and handling tasks. A job can be scheduled at a certain time.

Example:
```rust
use kvx::queue;

fn queue(store: &KeyValueStore) -> Result<(), kvx::Error> {
    let name = "job";
    let segment = Segment::parse(name).unwrap();
    let value = Value::from("value");

    // schedule a task
    queue.schedule_task(
        segment.into(),
        value,
        None,
        ScheduleMode::FinishOrReplaceExisting,
    )?;

    // claim a pending task
    let task_opt = queue.claim_scheduled_pending_task()?;

    if let Some(task) = task_opt {
        // do stuff...

        // then finish the task
        queue.finish_running_task(&Key::from(&task))?;
    }

    Ok(())
}


```



## Changelog

### Version 0.9.3
- Improve lockfile handling #62

### Version 0.9.2
- Always use a tempfile for new values on disk #60

### Version 0.9.1
- Keep lock files outside of scope dirs #58

### Version 0.9.0

Merged:
- Schedule tasks without finishing existing #56

This is a breaking change because the timestamp used for tasks
now uses milliseconds instead of seconds.

### Version 0.8.0

This release introduces a number of breaking changes. In particular,
we now use a dedicated type for `Namespace` and no longer prepend
a namespace `Segment` to keys. And the `Queue` implementation has
been overhauled.

- Add KeyValueStore::execute #38
- Use pretty printed JSON for values on disk #39
- Use Namespace type and support Namespace migrations #45
- Write tempfile and rename when using disk storage #46, #51
- Use a transactional queue #48, #50, #54

### Version 0.7.0

No functional changes were made, but the following updates were done
for the published crate on crates.io:
- Fix the reported license, it's BSD-3
- Update the GitHub repository link to the current location
- Update Readme files for better readability on crates.io

### Version 0.6.0

Breaking changes:
- Implicit .json extension for keys on disk were removed (see PR #32)