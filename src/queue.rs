use std::{
    fmt::{Display, Formatter},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Error, Key, KeyValueStore, KeyValueStoreBackend, ReadStore, Result, Scope, Segment, SegmentBuf,
    WriteStore,
};

fn current_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time-travel is not supported")
        .as_secs()
}

#[derive(Clone, Debug)]
enum TaskState {
    Pending(PendingTask),
    Running(RunningTask),
    Finished(FinishedTask),
}

impl TaskState {
    pub const SEPARATOR: char = '-';
}

impl TaskState {
    fn to_segment(&self) -> SegmentBuf {
        match self {
            TaskState::Pending(_) => PendingTask::segment(),
            TaskState::Running(_) => RunningTask::segment(),
            TaskState::Finished(_) => FinishedTask::segment(),
        }
    }
}

impl From<TaskState> for Key {
    fn from(task: TaskState) -> Self {
        let task_name: Key = match task.clone() {
            TaskState::Pending(t) => t.to_string().parse().unwrap(),
            TaskState::Running(t) => t.to_string().parse().unwrap(),
            TaskState::Finished(t) => t.to_string().parse().unwrap(),
        };

        task_name.with_namespace(task.to_segment())
    }
}

#[derive(Clone, Debug)]
struct PendingTask {
    pub task_name: SegmentBuf,
    pub schedule_timestamp: u64,
}

impl PendingTask {
    fn segment() -> SegmentBuf {
        SegmentBuf::from_str("pending").unwrap()
    }
}

impl TryFrom<Key> for PendingTask {
    type Error = Error;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        let (ts, name) = key
            .name()
            .as_str()
            .split_once(TaskState::SEPARATOR)
            .ok_or(Error::InvalidKey)?;
        Ok(PendingTask {
            task_name: Segment::parse(name)?.into(),
            schedule_timestamp: ts.parse().map_err(|_| Error::InvalidKey)?,
        })
    }
}

impl PartialEq for PendingTask {
    fn eq(&self, other: &Self) -> bool {
        self.task_name == other.task_name
    }
}

impl Display for PendingTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.schedule_timestamp,
            TaskState::SEPARATOR.encode_utf8(&mut [0; 4]),
            self.task_name,
        )
    }
}

#[derive(Clone, Debug)]
struct RunningTask {
    pub name: PendingTask,
    pub claim_timestamp: u64,
}

impl RunningTask {
    fn segment() -> SegmentBuf {
        SegmentBuf::from_str("running").unwrap()
    }
}

impl TryFrom<Key> for RunningTask {
    type Error = Error;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        let (ts, name) = key
            .name()
            .as_str()
            .split_once(TaskState::SEPARATOR)
            .ok_or(Error::InvalidKey)?;
        Ok(RunningTask {
            name: PendingTask::try_from(name.parse::<Key>()?)?,
            claim_timestamp: ts.parse().map_err(|_| Error::InvalidKey)?,
        })
    }
}

impl Display for RunningTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.claim_timestamp,
            TaskState::SEPARATOR.encode_utf8(&mut [0; 4]),
            self.name,
        )
    }
}

#[derive(Clone, Debug)]
struct FinishedTask {
    pub name: PendingTask,
    pub finish_timestamp: u64,
}

impl FinishedTask {
    fn segment() -> SegmentBuf {
        SegmentBuf::from_str("finished").unwrap()
    }
}

impl TryFrom<Key> for FinishedTask {
    type Error = Error;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        let (ts, name) = key
            .name()
            .as_str()
            .split_once(TaskState::SEPARATOR)
            .ok_or(Error::InvalidKey)?;
        Ok(FinishedTask {
            name: PendingTask::try_from(name.parse::<Key>()?)?,
            finish_timestamp: ts.parse().map_err(|_| Error::InvalidKey)?,
        })
    }
}

impl Display for FinishedTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.finish_timestamp,
            TaskState::SEPARATOR.encode_utf8(&mut [0; 4]),
            self.name,
        )
    }
}

#[derive(Clone, Debug)]
pub struct Task {
    state: TaskState,
    pub value: serde_json::Value,
}

impl Task {
    pub fn name(&self) -> SegmentBuf {
        self.state.to_segment()
    }
}

pub trait Queue {
    fn jobs_remaining(&self) -> Result<usize>;
    fn schedule_job(
        &self,
        task_name: SegmentBuf,
        value: serde_json::Value,
        timestamp: Option<u64>,
    ) -> Result<()>;
    fn finished_job(&self, task: Task) -> Result<()>;
    fn claim_job(&self) -> Option<Task>;
}

impl Queue for KeyValueStore {
    fn jobs_remaining(&self) -> Result<usize> {
        Ok(self
            .list_keys(&Scope::from_segment(PendingTask::segment()))?
            .len())
    }

    fn schedule_job(
        &self,
        task_name: SegmentBuf,
        value: serde_json::Value,
        timestamp: Option<u64>,
    ) -> Result<()> {
        let new_task = PendingTask {
            task_name,
            schedule_timestamp: timestamp.unwrap_or(current_time()),
        };

        self.transaction(
            &Scope::global(),
            Box::new(move |s: &dyn KeyValueStoreBackend| {
                let possible_exsisting: Option<PendingTask> = s
                    .list_keys(&Scope::from_segment(PendingTask::segment()))?
                    .into_iter()
                    .filter_map(|k| PendingTask::try_from(k).ok())
                    .find(|p| p.task_name == new_task.task_name);

                if let Some(exsisting) = possible_exsisting {
                    // reschedule exsisting task
                    s.move_value(
                        &TaskState::Pending(exsisting).into(),
                        &TaskState::Pending(new_task.clone()).into(),
                    )?;
                } else {
                    // store new task
                    s.store(&TaskState::Pending(new_task.clone()).into(), value.clone())?;
                }

                Ok(())
            }),
        )
    }

    fn finished_job(&self, task: Task) -> Result<()> {
        let finish_timestamp = current_time();
        match task.state.clone() {
            TaskState::Running(RunningTask { name, .. }) => {
                let finished = TaskState::Finished(FinishedTask {
                    name,
                    finish_timestamp,
                });
                self.move_value(&task.state.into(), &finished.into())
            }
            _ => todo!("not implemented for this state"),
        }
    }

    fn claim_job(&self) -> Option<Task> {
        let claimed: Arc<Mutex<Option<Task>>> = Arc::new(Mutex::new(None));

        let claimed_ref = claimed.clone();
        let claim_transaction = self.transaction(
            &Scope::global(),
            Box::new(move |s: &dyn KeyValueStoreBackend| {
                let now = current_time();
                let keys = s.list_keys(&Scope::from_segment(PendingTask::segment()))?;

                let candidate = keys
                    .into_iter()
                    .filter_map(|k| {
                        let state = PendingTask::try_from(k).ok()?;
                        if state.schedule_timestamp <= now {
                            Some(state)
                        } else {
                            None
                        }
                    })
                    .min_by_key(|s| s.schedule_timestamp);

                if let Some(name) = candidate {
                    let pending = TaskState::Pending(name.clone());
                    if let Some(value) = s.get(&pending.clone().into())? {
                        let running_task = Task {
                            state: TaskState::Running(RunningTask {
                                name,
                                claim_timestamp: now,
                            }),
                            value,
                        };

                        s.move_value(&pending.into(), &running_task.state.clone().into())?;

                        let mut claim = claimed_ref.lock().unwrap();
                        *claim = Some(running_task);
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
                eprintln!("failed to claim job {:?}", e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use kvx_types::Key;
    use serde_json::Value;
    use url::Url;

    use super::{FinishedTask, PendingTask, Queue, RunningTask};
    use crate::{KeyValueStore, ReadStore, Scope, Segment};

    fn queue_store() -> KeyValueStore {
        let storage_url = Url::parse("local://data").unwrap();

        KeyValueStore::new(&storage_url, Segment::parse("queue").unwrap()).unwrap()
    }

    #[test]
    fn queue_thread_workers() {
        let queue = queue_store();
        queue.inner.clear().unwrap();

        thread::scope(|s| {
            let create = s.spawn(|| {
                let queue = queue_store();

                for i in 1..=10 {
                    let name = &format!("job-{i}");
                    let segment = Segment::parse(name).unwrap();
                    let value = Value::from("value");

                    queue.schedule_job(segment.into(), value, None).unwrap();
                    println!("> Scheduled job {}", &name);
                }
            });

            create.join().unwrap();
            let keys = queue
                .list_keys(&Scope::from_segment(PendingTask::segment()))
                .unwrap();
            assert_eq!(keys.len(), 10);

            for i in 1..=10 {
                s.spawn(move || {
                    let queue = queue_store();

                    while queue.jobs_remaining().unwrap() > 0 {
                        if let Some(task) = queue.claim_job() {
                            let name = Into::<Key>::into(task.state.clone());
                            println!("- Worker {i} claimed job {name}");

                            std::thread::sleep(std::time::Duration::from_millis(5));
                            queue.finished_job(task).unwrap();
                            println!("+ Worker {i} finished job {name}");
                        }

                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                });
            }
        });

        let pending = queue
            .list_keys(&Scope::from_segment(PendingTask::segment()))
            .unwrap();
        assert_eq!(pending.len(), 0);

        let running = queue
            .list_keys(&Scope::from_segment(RunningTask::segment()))
            .unwrap();
        assert_eq!(running.len(), 0);

        let finished = queue
            .list_keys(&Scope::from_segment(FinishedTask::segment()))
            .unwrap();
        assert_eq!(finished.len(), 10);
    }
}
