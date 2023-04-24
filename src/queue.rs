use std::{
    fmt::{Display, Formatter},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    segment, Error, Key, KeyValueStore, KeyValueStoreBackend, ReadStore, Result, Scope, Segment,
    SegmentBuf, WriteStore,
};

fn current_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time-travel is not supported")
        .as_secs()
}

#[derive(Clone, Debug)]
enum TaskState {
    Submitted(SubmittedTask),
    Pending(PendingTask),
    Running(RunningTask),
    Finished(FinishedTask),
}

impl TaskState {
    pub const SEPARATOR: char = '-';
}

impl TaskState {
    fn to_segment(&self) -> &Segment {
        match self {
            TaskState::Pending(_) => PendingTask::SEGMENT,
            TaskState::Running(_) => RunningTask::SEGMENT,
            TaskState::Finished(_) => FinishedTask::SEGMENT,
            _ => todo!(),
        }
    }
}

impl From<TaskState> for Key {
    fn from(task: TaskState) -> Self {
        let keyname: Key = match task.clone() {
            TaskState::Pending(t) => t.to_string().parse().unwrap(),
            TaskState::Running(t) => t.to_string().parse().unwrap(),
            TaskState::Finished(t) => t.to_string().parse().unwrap(),
            _ => panic!(""),
        };

        keyname.with_namespace(task.to_segment())
    }
}

#[derive(Clone, Debug)]
pub struct SubmittedTask(pub SegmentBuf);

#[derive(Clone, Debug)]
struct PendingTask {
    pub keyname: SubmittedTask,
    pub schedule_timestamp: u64,
}

impl PendingTask {
    const SEGMENT: &Segment = segment!("pending");
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
            keyname: SubmittedTask(Segment::parse(name)?.into()),
            schedule_timestamp: ts.parse().map_err(|_| Error::InvalidKey)?,
        })
    }
}

impl Display for PendingTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.schedule_timestamp,
            TaskState::SEPARATOR.encode_utf8(&mut [0; 4]),
            self.keyname.0,
        )
    }
}

#[derive(Clone, Debug)]
struct RunningTask {
    pub name: PendingTask,
    pub claim_timestamp: u64,
}

impl RunningTask {
    const SEGMENT: &Segment = segment!("running");
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
    const SEGMENT: &Segment = segment!("finished");
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
    value: serde_json::Value,
}

impl Task {
    pub fn new(name: &Segment, value: serde_json::Value) -> Self {
        Self {
            state: TaskState::Submitted(SubmittedTask(name.into())),
            value,
        }
    }
}

pub trait Queue {
    fn jobs_remaining(&self) -> Result<usize>;
    fn schedule_job(&self, task: Task, timestamp: Option<u64>) -> Result<()>;
    fn finished_job(&self, task: Task) -> Result<()>;
    fn claim_job(&self) -> Option<Task>;
}

impl Queue for KeyValueStore {
    fn jobs_remaining(&self) -> Result<usize> {
        Ok(self
            .list_keys(&Scope::from_segment(PendingTask::SEGMENT))?
            .len())
    }

    fn schedule_job(&self, task: Task, timestamp: Option<u64>) -> Result<()> {
        let schedule_timestamp = timestamp.unwrap_or(current_time());
        match task.state.clone() {
            TaskState::Submitted(keyname) => {
                let scheduled = TaskState::Pending(PendingTask {
                    keyname,
                    schedule_timestamp,
                });
                self.store(&scheduled.into(), task.value)
            }
            TaskState::Pending(pending) => {
                let reschedule_transaction = self.transaction(
                    &Scope::global(), // TODO use queue scope?
                    Box::new(move |s: &dyn KeyValueStoreBackend| {
                        let pending_clone = pending.clone();
                        let state_clone = task.state.clone();
                        if s.get(&state_clone.clone().into())?.is_some() {
                            let rescheduled = TaskState::Pending(PendingTask {
                                keyname: pending_clone.keyname,
                                schedule_timestamp,
                            });

                            s.move_value(&state_clone.into(), &rescheduled.into())?;
                        }

                        Ok(())
                    }),
                );

                match reschedule_transaction {
                    Err(e) => {
                        eprintln!("failed to reschedule job {:?}", e);
                        todo!()
                    }
                    _ => Ok(()),
                }
            }
            TaskState::Running(running) => {
                let reschedule_transaction = self.transaction(
                    &Scope::global(), // TODO use queue scope?
                    Box::new(move |s: &dyn KeyValueStoreBackend| {
                        let running_clone = running.clone();
                        let state_clone = task.state.clone();
                        if s.get(&state_clone.clone().into())?.is_some() {
                            let rescheduled = TaskState::Pending(PendingTask {
                                keyname: running_clone.name.keyname,
                                schedule_timestamp,
                            });

                            s.move_value(&state_clone.into(), &rescheduled.into())?;
                        }

                        Ok(())
                    }),
                );

                match reschedule_transaction {
                    Err(e) => {
                        eprintln!("failed to reschedule job {:?}", e);
                        todo!()
                    }
                    _ => Ok(()),
                }
            }
            _ => unimplemented!("not implemented for this state"),
        }
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
            &Scope::global(), // TODO use queue scope?
            Box::new(move |s: &dyn KeyValueStoreBackend| {
                let now = current_time();
                let keys = s.list_keys(&Scope::from_segment(PendingTask::SEGMENT))?;

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

    use super::{FinishedTask, PendingTask, Queue, RunningTask, Task};
    use crate::{segment, KeyValueStore, ReadStore, Scope, Segment};

    fn queue_store() -> KeyValueStore {
        let storage_url = Url::parse("local://data").unwrap();

        KeyValueStore::new(&storage_url, segment!("queue")).unwrap()
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
                    let task = Task::new(segment, Value::from("value"));

                    queue.schedule_job(task, None).unwrap();
                    println!("> Scheduled job {}", &name);
                }
            });

            create.join().unwrap();
            let keys = queue
                .list_keys(&Scope::from_segment(PendingTask::SEGMENT))
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
            .list_keys(&Scope::from_segment(PendingTask::SEGMENT))
            .unwrap();
        assert_eq!(pending.len(), 0);

        let running = queue
            .list_keys(&Scope::from_segment(RunningTask::SEGMENT))
            .unwrap();
        assert_eq!(running.len(), 0);

        let finished = queue
            .list_keys(&Scope::from_segment(FinishedTask::SEGMENT))
            .unwrap();
        assert_eq!(finished.len(), 10);
    }
}
