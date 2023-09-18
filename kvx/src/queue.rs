use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    segment, Error, Key, KeyValueStore, KeyValueStoreBackend, Result, Scope, Segment, SegmentBuf,
};

const SEPARATOR: char = '-';

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time-travel is not supported")
        .as_secs()
}

struct TaskKey<'a> {
    pub name: Cow<'a, SegmentBuf>,
    pub timestamp: u64,
}

impl<'a> TaskKey<'a> {
    fn key(&self) -> Key {
        Key::from_str(&format!("{}{}{}", self.timestamp, SEPARATOR, self.name)).unwrap()
    }

    fn running_key(&self) -> Key {
        let mut key = self.key();
        key.add_super_scope(RunningTask::SEGMENT);
        key
    }

    fn pending_key(&self) -> Key {
        let mut key = self.key();
        key.add_super_scope(PendingTask::SEGMENT);
        key
    }
}

impl TryFrom<Key> for TaskKey<'_> {
    type Error = Error;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        let (ts, name) = key
            .name()
            .as_str()
            .split_once(SEPARATOR)
            .ok_or(Error::InvalidKey)?;
        Ok(TaskKey {
            name: Cow::Owned(Segment::parse(name)?.into()),
            timestamp: ts.parse().map_err(|_| Error::InvalidKey)?,
        })
    }
}

impl From<&PendingTask> for Key {
    fn from(p: &PendingTask) -> Self {
        let mut key = Key::from_str(&p.to_string()).unwrap();
        key.add_super_scope(PendingTask::SEGMENT);
        key
    }
}

impl From<&RunningTask> for Key {
    fn from(p: &RunningTask) -> Self {
        let mut key = Key::from_str(&p.to_string()).unwrap();
        key.add_super_scope(RunningTask::SEGMENT);
        key
    }
}

#[derive(Clone, Debug)]
pub struct PendingTask {
    pub name: SegmentBuf,
    pub timestamp: u64,
    pub value: serde_json::Value,
}

impl PendingTask {
    const SEGMENT: &Segment = segment!("pending");
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
            self.timestamp,
            SEPARATOR.encode_utf8(&mut [0; 4]),
            self.name,
        )
    }
}

#[derive(Clone, Debug)]
pub struct RunningTask {
    pub name: SegmentBuf,
    pub timestamp: u64,
    pub value: serde_json::Value,
}

impl RunningTask {
    const SEGMENT: &Segment = segment!("running");
}

impl Display for RunningTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.timestamp,
            SEPARATOR.encode_utf8(&mut [0; 4]),
            self.name,
        )
    }
}

/// Defines scheduling behaviour in case a task by the same name already exists.
#[derive(Clone, Copy, Debug)]
pub enum Existing {
    /// Store new task, replace old task if it exists.
    KeepNew,
    /// Store new task, replace old task if it exists, keep the soonest
    /// scheduled time.
    KeepNewScheduleSoonest,
    /// Reschedule existing, keeping the old value and ignoring the new value.
    ///
    /// Note: If the new value should be used, then use KeepNew instead.
    KeepOldWithNewTime,
    /// Keep existing, discard new.
    ///
    /// Note: Can be used to schedule a task only in case it's missing.
    KeepOld,
}

/// Defines scheduling behaviour in case a task by the same name is running.
#[derive(Clone, Copy, Debug)]
pub enum Running {
    /// Finish the running task if it exists.
    Finish,
    /// Keep the running task if it exists.
    Keep,
}

impl Running {
    pub fn must_be_finished(&self) -> bool {
        matches!(self, Running::Finish)
    }
}

pub trait Queue {
    const RESCHEDULE_AFTER: Duration = Duration::from_secs(15 * 60);

    fn pending_scope() -> Scope {
        Scope::from_segment(PendingTask::SEGMENT)
    }

    fn running_scope() -> Scope {
        Scope::from_segment(RunningTask::SEGMENT)
    }

    /// Returns the number of pending tasks remaining
    fn pending_tasks_remaining(&self) -> Result<usize>;

    /// Returns the number of running tasks
    fn running_tasks(&self) -> Result<usize>;

    /// Schedule a task.
    fn schedule_task(
        &self,
        name: SegmentBuf,
        value: serde_json::Value,
        timestamp: Option<u64>,
        existing: Existing,
        running: Running,
    ) -> Result<()>;

    /// Returns the scheduled time for the named task, if any.
    fn pending_task_scheduled(&self, name: SegmentBuf) -> Result<Option<u64>>;

    /// Marks a running task as finished. Fails if the task is not running.
    fn finish_running_task(&self, running: RunningTask) -> Result<()>;

    /// Claims the next scheduled pending task, if any.
    fn claim_scheduled_pending_task(&self) -> Result<Option<RunningTask>>;

    /// Reschedules running tasks that have timed out.
    fn reschedule_long_running_tasks(&self, reschedule_after: Option<&Duration>) -> Result<()>;
}

impl Queue for KeyValueStore {
    fn pending_tasks_remaining(&self) -> Result<usize> {
        self.execute(&Self::pending_scope(), |kv| {
            kv.list_keys(&Self::pending_scope()).map(|list| list.len())
        })
    }

    fn running_tasks(&self) -> Result<usize> {
        self.execute(&Self::pending_scope(), |kv| {
            kv.list_keys(&Self::running_scope()).map(|list| list.len())
        })
    }

    fn schedule_task(
        &self,
        name: SegmentBuf,
        value: serde_json::Value,
        timestamp: Option<u64>,
        mode_existing: Existing,
        running: Running,
    ) -> Result<()> {
        let mut new_task = PendingTask {
            name,
            timestamp: timestamp.unwrap_or(now()),
            value,
        };
        let new_task_key = Key::from(&new_task);

        self.transaction(
            &Scope::global(),
            &mut move |s: &dyn KeyValueStoreBackend| {
                // Finish the matching running task (if any) if requested
                // to do so.
                if running.must_be_finished() {
                    if let Some(running) = s
                        .list_keys(&Scope::from_segment(RunningTask::SEGMENT))?
                        .into_iter()
                        .filter_map(|k| TaskKey::try_from(k).ok())
                        .find(|running| running.name.as_ref() == &new_task.name)
                        .map(|tk| tk.running_key())
                    {
                        s.delete(&running)?;
                    }
                }

                // If there is an existing pending task for this,
                // then we need to figure out what to do with it.
                if let Some(existing) = s
                    .list_keys(&Scope::from_segment(PendingTask::SEGMENT))?
                    .into_iter()
                    .filter_map(|k| TaskKey::try_from(k).ok())
                    .find(|p| p.name.as_ref() == &new_task.name)
                {
                    let pending_key = existing.pending_key();

                    match mode_existing {
                        Existing::KeepOld => {
                            // nothing to do
                            Ok(())
                        }
                        Existing::KeepOldWithNewTime => {
                            // reschedule existing task
                            // note that the due time is part of the key, not the content
                            s.move_value(&pending_key, &new_task_key)
                        }
                        Existing::KeepNew => {
                            s.delete(&pending_key)?;
                            s.store(&new_task_key, new_task.value.clone())
                        }
                        Existing::KeepNewScheduleSoonest => {
                            new_task.timestamp = new_task.timestamp.min(existing.timestamp);

                            let new_task_key = Key::from(&new_task);
                            s.delete(&pending_key)?;
                            s.store(&new_task_key, new_task.value.clone())
                        }
                    }
                } else {
                    // There was no matching pending task, just store
                    // this new task.
                    s.store(&new_task_key, new_task.value.clone())
                }
            },
        )
    }

    fn finish_running_task(&self, running: RunningTask) -> Result<()> {
        let running_key = Key::from(&running);

        // The scopes for running and finished differ, so we need a global lock
        let lock_scope = Scope::global();

        self.execute(&lock_scope, |kv| {
            if kv.has(&running_key)? {
                kv.delete(&running_key)
            } else {
                Err(Error::Other(format!(
                    "Cannot finish task {}. It is not running.",
                    &running_key
                )))
            }
        })
    }

    fn claim_scheduled_pending_task(&self) -> Result<Option<RunningTask>> {
        self.execute(&Scope::global(), |kv| {
            let now = now();

            if let Some(pending) = kv
                .list_keys(&Scope::from_segment(PendingTask::SEGMENT))?
                .into_iter()
                .filter_map(|k| TaskKey::try_from(k).ok())
                .filter(|tk| tk.timestamp <= now)
                .min_by_key(|tk| tk.timestamp)
            {
                let pending_key = pending.pending_key();
                let running_key = pending.running_key();

                if let Some(value) = kv.get(&pending_key)? {
                    let running_task = RunningTask {
                        name: pending.name.into_owned(),
                        timestamp: now,
                        value,
                    };

                    kv.move_value(&pending_key, &running_key)?;

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
        let now = now();

        let reschedule_after = reschedule_after.unwrap_or(&KeyValueStore::RESCHEDULE_AFTER);
        let reschedule_timeout = now - reschedule_after.as_secs();

        self.transaction(
            &Scope::global(),
            &mut move |s: &dyn KeyValueStoreBackend| {
                s.list_keys(&Scope::from_segment(RunningTask::SEGMENT))?
                    .into_iter()
                    .filter_map(|k| {
                        let task = TaskKey::try_from(k).ok()?;
                        if task.timestamp <= reschedule_timeout {
                            Some(task)
                        } else {
                            None
                        }
                    })
                    .for_each(|tk| {
                        let running_key = tk.running_key();

                        let pending_key = TaskKey {
                            name: Cow::Borrowed(&tk.name),
                            timestamp: now,
                        }
                        .pending_key();

                        let _ = s.move_value(&running_key, &pending_key);
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
                        .filter_map(|k| TaskKey::try_from(k).ok())
                        .find(|p| p.name.as_ref() == &name)
                        .map(|p| p.timestamp)
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use kvx_macros::segment;
    use kvx_types::SegmentBuf;
    use serde_json::Value;
    use url::Url;

    use super::{PendingTask, Queue};
    use crate::{
        queue::{now, Existing, Running},
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
                        .schedule_task(
                            segment.into(),
                            value,
                            None,
                            Existing::KeepOldWithNewTime,
                            Running::Finish,
                        )
                        .unwrap();
                    println!("> Scheduled job {}", &name);
                }
            });

            create.join().unwrap();
            let keys = queue
                .list_keys(&Scope::from_segment(PendingTask::SEGMENT))
                .unwrap();
            assert_eq!(keys.len(), 10);

            for _i in 1..=10 {
                s.spawn(move || {
                    let queue = queue_store("test_queue");

                    while queue.pending_tasks_remaining().unwrap() > 0 {
                        if let Some(running_task) = queue.claim_scheduled_pending_task().unwrap() {
                            queue.finish_running_task(running_task).unwrap();
                        }

                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                });
            }
        });

        let pending = queue.pending_tasks_remaining().unwrap();
        assert_eq!(pending, 0);

        let running = queue.running_tasks().unwrap();
        assert_eq!(running, 0);
    }

    #[test]
    fn test_reschedule_long_running() {
        let queue = queue_store("test_cleanup_queue");
        queue.inner.clear().unwrap();

        let name = "job";
        let segment = Segment::parse(name).unwrap();
        let value = Value::from("value");

        queue
            .schedule_task(
                segment.into(),
                value,
                None,
                Existing::KeepOldWithNewTime,
                Running::Finish,
            )
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
            .schedule_task(
                segment.into(),
                value,
                None,
                Existing::KeepNew,
                Running::Finish,
            )
            .unwrap();
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

        // Get the task
        let running_task = queue.claim_scheduled_pending_task().unwrap().unwrap();
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);
        assert_eq!(queue.running_tasks().unwrap(), 1);

        // Finish the task and reschedule
        // queue.finish_running_task(task, Some(rescheduled)).unwrap();
        queue
            .schedule_task(
                running_task.name,
                running_task.value,
                Some(now()),
                Existing::KeepNew,
                Running::Finish,
            )
            .unwrap();

        // There should now be a new pending task, and the
        // running task should be removed.
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);
        assert_eq!(queue.running_tasks().unwrap(), 0);

        // Get and finish the pending task, but do not reschedule it
        let running_task = queue.claim_scheduled_pending_task().unwrap().unwrap();
        assert_eq!(queue.pending_tasks_remaining().unwrap(), 0);
        queue.finish_running_task(running_task).unwrap();

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

        let in_a_while = now() + 180;

        // Schedule a task, and then schedule again replacing the old
        {
            queue
                .schedule_task(
                    name.clone(),
                    value_1.clone(),
                    None,
                    Existing::KeepNew,
                    Running::Finish,
                )
                .unwrap();
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

            // Schedule again, replacing the existing task
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    None,
                    Existing::KeepNew,
                    Running::Finish,
                )
                .unwrap();
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

            // We should have one task and the value should match the new task.
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_2);
        }

        // Schedule a task, and then schedule again keeping the old
        {
            queue
                .schedule_task(
                    name.clone(),
                    value_1.clone(),
                    None,
                    Existing::KeepNew,
                    Running::Finish,
                )
                .unwrap();
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    Some(in_a_while),
                    Existing::KeepOld,
                    Running::Finish,
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
                .schedule_task(
                    name.clone(),
                    value_1.clone(),
                    None,
                    Existing::KeepNew,
                    Running::Finish,
                )
                .unwrap();

            // we expect one pending task
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);

            // reschedule that task to 3 minutes from now
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    Some(in_a_while),
                    Existing::KeepOldWithNewTime,
                    Running::Finish,
                )
                .unwrap();

            // when we try to claim a scheduled task, we should
            // get nothing because our one and only task is not
            // due yet.
            assert!(queue.claim_scheduled_pending_task().unwrap().is_none());

            // reschedule that task to now
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    None,
                    Existing::KeepOldWithNewTime,
                    Running::Finish,
                )
                .unwrap();

            // and when we get it its value should match the original task
            // that was rescheduled two times.
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_1);
        }

        // Schedule a task, then schedule a new task keeping the same time and name.
        {
            queue
                .schedule_task(
                    name.clone(),
                    value_1.clone(),
                    None,
                    Existing::KeepNew,
                    Running::Finish,
                )
                .unwrap();
            queue
                .schedule_task(
                    name.clone(),
                    value_2.clone(),
                    None,
                    Existing::KeepNew,
                    Running::Finish,
                )
                .unwrap();

            // We should see only task - since name and time together are the unique key
            // The value should match the new task.
            assert_eq!(queue.pending_tasks_remaining().unwrap(), 1);
            let task = queue.claim_scheduled_pending_task().unwrap().unwrap();
            assert_eq!(task.value, value_2);
        }
    }
}
