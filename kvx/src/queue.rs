use std::{
    fmt::{Display, Formatter},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    segment, Error, Key, KeyValueStore, KeyValueStoreBackend, Result, Scope, Segment, SegmentBuf,
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
    fn super_scope(&self) -> &Segment {
        match self {
            TaskState::Pending(_) => PendingTask::SEGMENT,
            TaskState::Running(_) => RunningTask::SEGMENT,
            TaskState::Finished(_) => FinishedTask::SEGMENT,
        }
    }
}

impl From<TaskState> for Key {
    fn from(task: TaskState) -> Self {
        let mut name: Key = match task.clone() {
            TaskState::Pending(t) => t.to_string().parse().unwrap(),
            TaskState::Running(t) => t.to_string().parse().unwrap(),
            TaskState::Finished(t) => t.to_string().parse().unwrap(),
        };

        name.add_super_scope(task.super_scope());

        name
    }
}

#[derive(Clone, Debug)]
struct PendingTask {
    pub name: SegmentBuf,
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
            name: Segment::parse(name)?.into(),
            schedule_timestamp: ts.parse().map_err(|_| Error::InvalidKey)?,
        })
    }
}

impl PartialEq for PendingTask {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Display for PendingTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.schedule_timestamp,
            TaskState::SEPARATOR.encode_utf8(&mut [0; 4]),
            self.name,
        )
    }
}

#[derive(Clone, Debug)]
struct RunningTask {
    pub task_name: PendingTask,
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
            task_name: PendingTask::try_from(name.parse::<Key>()?)?,
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
            self.task_name,
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

/// Defines a task which may be pending, running or finished.
#[derive(Clone, Debug)]
pub struct Task {
    state: TaskState,
    pub value: serde_json::Value,
}

impl Task {
    pub fn name(&self) -> &Segment {
        self.state.super_scope()
    }
}

/// Defines a task which is optionally used to reschedule a finished task.
///
/// Takes the name from the finished task. A new value can be specified. If
/// it is omitted then the value from the finished task will be re-used.
#[derive(Clone, Debug)]
pub struct RescheduledTask {
    pub new_value: Option<serde_json::Value>,
    pub schedule_timestamp: u64,
}

/// Defines scheduling behaviour in case a task by the same name already exists.
#[derive(Clone, Copy, Debug)]
pub enum Existing {
    /// Reschedule existing, keeping the old value and ignoring the new value.
    ///
    /// NOTE: If the new value should be used, then use KeepNew instead.
    Reschedule,
    /// Store new task, replace old task if it exists.
    KeepNew,
    /// Keep existing, discard new.
    KeepOld,
    /// Keep existing and add new.
    ///
    /// NOTE: The task key is determined by its name and scheduled time.
    ///       If both the existing and the new scheduled time are the
    ///       same, then this results in replacing the existing task.
    KeepBoth,
}

pub trait Queue {
    const RESCHEDULE_AFTER: Duration = Duration::from_secs(15 * 60);
    const REMOVE_AFTER: Duration = Duration::from_secs(7 * 24 * 60 * 60);

    fn pending_scope() -> Scope {
        Scope::from_segment(PendingTask::SEGMENT)
    }

    /// Returns the number of pending tasks remaining
    fn pending_tasks_remaining(&self) -> Result<usize>;

    /// Schedule a task.
    fn schedule_task(
        &self,
        name: SegmentBuf,
        value: serde_json::Value,
        timestamp: Option<u64>,
        mode: Existing,
    ) -> Result<()>;

    /// Returns the scheduled time for the named task, if any.
    fn pending_task_scheduled(&self, name: SegmentBuf) -> Result<Option<u64>>;

    /// Marks a running task as finished. Fails if the task is not running.
    fn finish_running_task(
        &self,
        task: Task,
        rescheduled_task: Option<RescheduledTask>,
    ) -> Result<()>;

    /// Claims the next scheduled pending task, if any.
    fn claim_scheduled_pending_task(&self) -> Result<Option<Task>>;

    /// Reschedules running tasks that have timed out.
    fn reschedule_long_running_tasks(&self, reschedule_after: Option<&Duration>) -> Result<()>;

    /// Cleans finished tasks.
    fn clean_up_finished_tasks(&self, remove_after: Option<&Duration>) -> Result<()>;
}

impl Queue for KeyValueStore {
    fn pending_tasks_remaining(&self) -> Result<usize> {
        self.execute(&Self::pending_scope(), |kv| {
            kv.list_keys(&Self::pending_scope()).map(|list| list.len())
        })
    }

    fn schedule_task(
        &self,
        name: SegmentBuf,
        value: serde_json::Value,
        timestamp: Option<u64>,
        mode: Existing,
    ) -> Result<()> {
        let new_task = PendingTask {
            name,
            schedule_timestamp: timestamp.unwrap_or(current_time()),
        };

        self.transaction(
            &Scope::global(),
            &mut move |s: &dyn KeyValueStoreBackend| {
                let possible_existing: Option<PendingTask> = s
                    .list_keys(&Scope::from_segment(PendingTask::SEGMENT))?
                    .into_iter()
                    .filter_map(|k| PendingTask::try_from(k).ok())
                    .find(|p| p.name == new_task.name);

                if let Some(existing) = possible_existing {
                    match mode {
                        Existing::KeepOld => {
                            // nothing to do
                            Ok(())
                        }
                        Existing::KeepBoth => {
                            // just save the new task, possibly overwriting the
                            // existing task if the name and scheduled time are
                            // the same
                            s.store(&TaskState::Pending(new_task.clone()).into(), value.clone())
                        }
                        Existing::Reschedule => {
                            // reschedule existing task
                            s.move_value(
                                &TaskState::Pending(existing).into(),
                                &TaskState::Pending(new_task.clone()).into(),
                            )
                        }
                        Existing::KeepNew => {
                            s.delete(&TaskState::Pending(existing).into())?;
                            s.store(&TaskState::Pending(new_task.clone()).into(), value.clone())
                        }
                    }
                } else {
                    // store new task
                    s.store(&TaskState::Pending(new_task.clone()).into(), value.clone())
                }
            },
        )
    }

    fn finish_running_task(&self, task: Task, rescheduled: Option<RescheduledTask>) -> Result<()> {
        let finish_timestamp = current_time();
        match task.state.clone() {
            TaskState::Running(RunningTask { task_name, .. }) => {
                let finished = TaskState::Finished(FinishedTask {
                    name: task_name,
                    finish_timestamp,
                });

                let running: Key = task.state.into();
                let finished: Key = finished.into();

                // Note in this case, the scopes differ, so we need a global lock
                let lock_scope = Scope::global();

                let pending = rescheduled.map(|r| {
                    (
                        TaskState::Pending(PendingTask {
                            name: running.name().to_owned(),
                            schedule_timestamp: r.schedule_timestamp,
                        }),
                        r.new_value,
                    )
                });

                self.execute(&lock_scope, move |kv| {
                    if let Some((pending, value_opt)) = pending.clone() {
                        let pending_key: Key = pending.into();
                        let value = match value_opt {
                            Some(value) => value,
                            None => kv.get(&running)?.ok_or(Error::Other(format!(
                                "cannot find existing value for rescheduling task: {}",
                                pending_key
                            )))?,
                        };

                        kv.store(&pending_key, value)?;
                    }

                    kv.move_value(&running, &finished)?;
                    Ok(())
                })
            }
            _ => Err(Error::Other(format!(
                "Cannot finish task {}. It is not running.",
                task.name()
            ))),
        }
    }

    fn claim_scheduled_pending_task(&self) -> Result<Option<Task>> {
        self.execute(&Scope::global(), |kv| {
            let now = current_time();
            let keys = kv.list_keys(&Scope::from_segment(PendingTask::SEGMENT))?;

            let candidate = keys
                .into_iter()
                .filter_map(|k| {
                    let task = PendingTask::try_from(k).ok()?;
                    if task.schedule_timestamp <= now {
                        Some(task)
                    } else {
                        None
                    }
                })
                .min_by_key(|s| s.schedule_timestamp);

            if let Some(name) = candidate {
                let pending = TaskState::Pending(name.clone());
                if let Some(value) = kv.get(&pending.clone().into())? {
                    let running_task = Task {
                        state: TaskState::Running(RunningTask {
                            task_name: name,
                            claim_timestamp: now,
                        }),
                        value,
                    };

                    kv.move_value(&pending.into(), &running_task.state.clone().into())?;

                    Ok(Some(running_task))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })
    }

    fn reschedule_long_running_tasks(&self, reschedule_after: Option<&Duration>) -> Result<()> {
        let now = current_time();

        let reschedule_after = reschedule_after.unwrap_or(&KeyValueStore::RESCHEDULE_AFTER);
        let reschedule_timeout = now - reschedule_after.as_secs();

        self.transaction(
            &Scope::global(),
            &mut move |s: &dyn KeyValueStoreBackend| {
                s.list_keys(&Scope::from_segment(RunningTask::SEGMENT))?
                    .into_iter()
                    .filter_map(|k| {
                        let task = RunningTask::try_from(k).ok()?;
                        if task.claim_timestamp <= reschedule_timeout {
                            Some(task)
                        } else {
                            None
                        }
                    })
                    .for_each(|running: RunningTask| {
                        let pending = PendingTask {
                            name: running.task_name.name.clone(),
                            schedule_timestamp: now,
                        };

                        let _ = s.move_value(
                            &TaskState::Running(running).into(),
                            &TaskState::Pending(pending).into(),
                        );
                    });

                Ok(())
            },
        )
    }

    fn clean_up_finished_tasks(&self, remove_after: Option<&Duration>) -> Result<()> {
        let now = current_time();

        let remove_after = remove_after.unwrap_or(&KeyValueStore::REMOVE_AFTER);
        let remove_timeout = now - remove_after.as_secs();

        self.transaction(
            &Scope::global(),
            &mut move |s: &dyn KeyValueStoreBackend| {
                s.list_keys(&Scope::from_segment(FinishedTask::SEGMENT))?
                    .into_iter()
                    .filter_map(|k| {
                        let task = FinishedTask::try_from(k).ok()?;
                        if task.finish_timestamp <= remove_timeout {
                            Some(task)
                        } else {
                            None
                        }
                    })
                    .for_each(|finished: FinishedTask| {
                        let _ = s.delete(&TaskState::Finished(finished).into());
                    });

                Ok(())
            },
        )
    }

    fn pending_task_scheduled(&self, name: SegmentBuf) -> Result<Option<u64>> {
        self.execute(&Self::pending_scope(), |kv| {
            kv.list_keys(&Scope::from_segment(PendingTask::SEGMENT))
                .map(|keys| {
                    keys.into_iter()
                        .filter_map(|k| PendingTask::try_from(k).ok())
                        .find(|p| p.name == name)
                        .map(|p| p.schedule_timestamp)
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use kvx_macros::segment;
    use kvx_types::{Key, SegmentBuf};
    use serde_json::Value;
    use url::Url;

    use super::{FinishedTask, PendingTask, Queue, RunningTask};
    use crate::{
        queue::{current_time, Existing, RescheduledTask},
        KeyValueStore, Namespace, ReadStore, Scope, Segment,
    };

    fn queue_store(ns: &str) -> KeyValueStore {
        let storage_url = Url::parse("local://data").unwrap();

        KeyValueStore::new(&storage_url, Namespace::parse(ns).unwrap()).unwrap()
    }

    #[test]
    fn queue_thread_workers() {
        let queue = queue_store("test_queue");
        queue.inner.clear().unwrap();

        thread::scope(|s| {
            let create = s.spawn(|| {
                let queue = queue_store("test_queue");

                for i in 1..=10 {
                    let name = &format!("job-{i}");
                    let segment = Segment::parse(name).unwrap();
                    let value = Value::from("value");

                    queue
                        .schedule_task(segment.into(), value, None, Existing::Reschedule)
                        .unwrap();
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
                    let queue = queue_store("test_queue");

                    while queue.pending_tasks_remaining().unwrap() > 0 {
                        if let Some(task) = queue.claim_scheduled_pending_task().unwrap() {
                            let name = Into::<Key>::into(task.state.clone());
                            println!("- Worker {i} claimed job {name}");

                            std::thread::sleep(std::time::Duration::from_millis(5));
                            queue.finish_running_task(task, None).unwrap();
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

    #[test]
    fn test_reschedule_long_running() {
        let queue = queue_store("test_cleanup_queue");
        queue.inner.clear().unwrap();

        let name = "job";
        let segment = Segment::parse(name).unwrap();
        let value = Value::from("value");

        queue
            .schedule_task(segment.into(), value, None, Existing::Reschedule)
            .unwrap();

        assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

        let job = queue.claim_scheduled_pending_task().unwrap();

        assert!(job.is_some());
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);

        let job = queue.claim_scheduled_pending_task().unwrap();

        assert!(job.is_none());

        queue
            .reschedule_long_running_tasks(Some(&Duration::from_secs(0)))
            .unwrap();

        let existing = queue.pending_task_scheduled(segment.into()).unwrap();

        assert!(existing.is_some());
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

        let job = queue.claim_scheduled_pending_task().unwrap();

        assert!(job.is_some());
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);
    }

    #[test]
    fn test_reschedule_finished_task() {
        let queue = queue_store("test_cleanup_queue");
        queue.inner.clear().unwrap();

        let name = "task";
        let segment = Segment::parse(name).unwrap();
        let value = Value::from("value");

        // Schedule the task
        queue
            .schedule_task(segment.into(), value, None, Existing::KeepNew)
            .unwrap();
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

        // Get the task
        let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);

        // Finish the task and reschedule
        let rescheduled = RescheduledTask {
            new_value: None,
            schedule_timestamp: current_time(),
        };
        queue.finish_running_task(task, Some(rescheduled)).unwrap();

        // There should now be a new pending task
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

        // Get and finish the pending task, but do not reschedule it
        let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);
        queue.finish_running_task(task, None).unwrap();

        // There should not be a new pending task
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);
    }

    #[test]
    fn test_schedule_with_existing_task() {
        let queue = queue_store("test_cleanup_queue");
        queue.inner.clear().unwrap();

        let name: SegmentBuf = segment!("task").into();
        let value_1 = Value::from("value_1");
        let value_2 = Value::from("value_2");

        let in_a_while = current_time() + 180;

        // Schedule a task, and then schedule again replacing the old
        {
            queue
                .schedule_task(name.clone(), value_1.clone(), None, Existing::KeepNew)
                .unwrap();
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

            // Schedule again, replacing the existing task
            queue
                .schedule_task(name.clone(), value_2.clone(), None, Existing::KeepNew)
                .unwrap();
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

            // We should have one task and the value should match the new task.
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_2);
        }

        // Schedule a task, and then schedule again keeping the old
        {
            queue
                .schedule_task(name.clone(), value_1.clone(), None, Existing::KeepNew)
                .unwrap();
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    Some(in_a_while),
                    Existing::KeepOld,
                )
                .unwrap();

            // there should be only one task, it should not be rescheduled,
            // so we get get it and its value should match old.
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_1);
        }

        // Schedule a task, and then schedule again rescheduling it
        {
            queue
                .schedule_task(name.clone(), value_1.clone(), None, Existing::KeepNew)
                .unwrap();

            // we expect one pending task
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

            // reschedule that task to 3 minutes from now
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    Some(in_a_while),
                    Existing::Reschedule,
                )
                .unwrap();

            // when we try to claim a scheduled task, we should
            // get nothing because our one and only task is not
            // due yet.
            assert!(queue.claim_scheduled_pending_task().unwrap().is_none());

            // reschedule that task to now
            queue
                .schedule_task(name.clone(), value_2.clone(), None, Existing::Reschedule)
                .unwrap();

            // and when we get it its value should match the original task
            // that was rescheduled two times.
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_1);
        }

        // Schedule a task, then schedule a new task keeping both.
        {
            // In case we use the same time..
            queue
                .schedule_task(name.clone(), value_1.clone(), None, Existing::KeepNew)
                .unwrap();
            queue
                .schedule_task(name.clone(), value_2.clone(), None, Existing::KeepBoth)
                .unwrap();

            // We should see only task - since name and time together are the unique key
            // The value should match the new task.
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_2);

            // In case we use a new time, then we should have two pending tasks.
            queue
                .schedule_task(name.clone(), value_1.clone(), None, Existing::KeepNew)
                .unwrap();
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    Some(in_a_while),
                    Existing::KeepBoth,
                )
                .unwrap();

            // Two pending tasks, we can get only one and its value matches old
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 2);
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_1);
            assert!(queue.claim_scheduled_pending_task().unwrap().is_none());
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);
        }
    }
}
