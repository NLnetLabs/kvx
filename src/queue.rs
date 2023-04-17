use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    key::SegmentBuf, Error, Key, KeyValueStore, KeyValueStoreBackend, ReadStore, Result, Scope,
    WriteStore,
};

fn get_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time-travel is not supported")
        .as_secs()
}

#[derive(Clone, Debug)]
pub enum TaskState {
    Pending,
    Running,
    Finished,
}

impl TaskState {
    fn as_segment(&self) -> SegmentBuf {
        match self {
            TaskState::Pending => SegmentBuf::from_str("pending"),
            TaskState::Running => SegmentBuf::from_str("running"),
            TaskState::Finished => SegmentBuf::from_str("finished"),
        }
        .unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct Task {
    pub key: String,
    pub value: serde_json::Value,
    pub schedule_timestamp: u64,
    pub state: TaskState,
}

impl Task {
    fn get_key(&self) -> Key {
        format!("{}-{}", self.schedule_timestamp, self.key)
            .parse::<Key>()
            .unwrap()
            .with_sub_scope(self.state.as_segment())
    }
}

pub trait Queue {
    fn jobs_remaining(&self) -> Result<usize>;
    fn schedule_job(&self, task: Task) -> Result<()>;
    fn finished_job(&self, task: Task) -> Result<()>;
    fn claim_job(&self) -> Option<Task>;
}

impl Queue for KeyValueStore {
    fn jobs_remaining(&self) -> Result<usize> {
        Ok(self
            .list_keys(&Scope::from_segment(TaskState::Pending.as_segment()))?
            .len())
    }

    fn schedule_job(&self, task: Task) -> Result<()> {
        self.store(&task.get_key(), task.value)
    }

    fn finished_job(&self, task: Task) -> Result<()> {
        let mut finished_task = task.clone();
        finished_task.state = TaskState::Finished;

        self.move_value(&task.get_key(), &finished_task.get_key())
    }

    fn claim_job(&self) -> Option<Task> {
        let claimed: Arc<Mutex<Option<Task>>> = Arc::new(Mutex::new(None));

        let claimed_ref = claimed.clone();
        let claim_transaction = self.transaction(
            &Scope::global(),
            Box::new(move |s: &dyn KeyValueStoreBackend| {
                let now = get_ts();

                let keys = s.list_keys(&Scope::from_segment(TaskState::Pending.as_segment()))?;

                let mut candidates = Vec::new();

                for key in keys.iter() {
                    let key_name = key.name();
                    let (ts_str, task_key) =
                        key_name.as_str().split_once('-').ok_or(Error::InvalidKey)?;
                    let ts: u64 = ts_str.parse().map_err(|_| Error::InvalidKey)?;

                    if ts <= now {
                        candidates.push((ts, task_key, key));
                    }
                }

                candidates.sort_by(|a, b| a.0.cmp(&b.0));

                if let Some((schedule_timestamp, task_key, key)) = candidates.pop() {
                    if let Some(value) = s.get(key)? {
                        let task = Task {
                            key: task_key.to_string(),
                            value,
                            schedule_timestamp,
                            state: TaskState::Running,
                        };

                        s.move_value(key, &task.get_key())?;

                        let mut claim = claimed_ref.lock().unwrap();
                        *claim = Some(task);
                    }
                }

                Ok(())
            }),
        );

        match claim_transaction {
            Ok(_) => {
                let result = claimed.lock().unwrap();
                result.to_owned()
            }
            Err(e) => {
                println!("Failed claiming job {:?}", e);

                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{get_ts, Queue, Task, TaskState};
    use crate::{key::SegmentBuf, KeyValueStore, ReadStore, Scope, WriteStore};
    use serde_json::json;
    use std::{str::FromStr, thread};
    use url::Url;

    fn get_queue_store() -> KeyValueStore {
        let storage_url = Url::parse("local://data").unwrap();

        KeyValueStore::new(&storage_url, SegmentBuf::from_str("queue").unwrap()).unwrap()
    }

    #[test]
    fn queue_thread_workers() {
        let queue_store = get_queue_store();
        queue_store.clear().unwrap();

        thread::scope(|s| {
            let create = s.spawn(|| {
                let queue = get_queue_store();

                for i in 1..=10 {
                    let key: String = format!("job-{i}");
                    let task = Task {
                        value: json!({ "name": &key }),
                        key: key,
                        schedule_timestamp: get_ts(),
                        state: TaskState::Pending,
                    };

                    println!("> Scheduled job {}", &task.key);
                    queue.schedule_job(task).unwrap();
                }
            });

            create.join().unwrap();
            let keys = queue_store
                .list_keys(&Scope::from_segment(TaskState::Pending.as_segment()))
                .unwrap();
            assert_eq!(keys.len(), 10);

            for i in 1..=10 {
                s.spawn(move || {
                    let queue = get_queue_store();

                    while queue.jobs_remaining().unwrap() > 0 {
                        if let Some(task) = queue.claim_job() {
                            let key = task.key.clone();
                            println!("- Worker {i} claimed job {key}");
                            std::thread::sleep(std::time::Duration::from_millis(5));
                            queue.finished_job(task).unwrap();
                            println!("+ Worker {i} finished job {key}");
                        }
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                });
            }
        });

        let keys = queue_store
            .list_keys(&Scope::from_segment(TaskState::Finished.as_segment()))
            .unwrap();
        assert_eq!(keys.len(), 10);
    }
}
