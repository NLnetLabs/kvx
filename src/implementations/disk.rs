use std::{
    fs,
    fs::{File, OpenOptions},
    ops::{Deref, DerefMut},
    path::{Component, Path, PathBuf},
    thread,
    time::Duration,
};

use serde_json::Value;

use crate::{
    key::{Key, Scope, SegmentBuf},
    Error, KeyValueStoreBackend, ReadStore, Result, TransactionCallback, WriteStore,
};

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Disk {
    root: PathBuf,
}

impl Disk {
    pub fn new(path: &str, namespace: &str) -> Result<Self> {
        let root = PathBuf::from(path).join(namespace);
        if !root.try_exists().unwrap_or_default() {
            fs::create_dir_all(&root)?;
        }
        Ok(Disk { root })
    }
}

impl ReadStore for Disk {
    fn has(&self, key: &Key) -> Result<bool> {
        let exists = key.as_path(&self.root).exists();
        Ok(exists)
    }

    fn has_scope(&self, scope: &Scope) -> Result<bool> {
        let exists = scope.as_path(&self.root).try_exists()?;
        Ok(exists)
    }

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        let path = key.as_path(&self.root);
        if path.exists() {
            let value = fs::read_to_string(key.as_path(&self.root))?;
            let value: Value = serde_json::from_str(&value)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn list_keys(&self, scope: &Scope) -> Result<Vec<Key>> {
        let path = scope.as_path(&self.root);
        if !path.exists() {
            return Ok(vec![]);
        }

        list_files_recursive(scope.as_path(&self.root))?
            .into_iter()
            .map(|path| path.as_key(&self.root))
            .collect()
    }

    fn list_scopes(&self) -> Result<Vec<Scope>> {
        list_dirs_recursive(Scope::global().as_path(&self.root))?
            .into_iter()
            .map(|path| path.as_scope(&self.root))
            .collect()
    }
}

impl WriteStore for Disk {
    fn store(&self, key: &Key, value: Value) -> Result<()> {
        let path = key.as_path(&self.root);
        let dir = key.scope().as_path(&self.root);

        if !dir.try_exists().unwrap_or_default() {
            fs::create_dir_all(dir)?;
        }

        fs::write(path, value.to_string().as_bytes())?;

        Ok(())
    }

    fn move_value(&self, from: &Key, to: &Key) -> Result<()> {
        let from_path = from.as_path(&self.root);
        let to_path = to.as_path(&self.root);

        let dir = to.scope().as_path(&self.root);
        if !dir.try_exists().unwrap_or_default() {
            fs::create_dir_all(dir)?;
        }

        fs::rename(&from_path, to_path)?;
        remove_empty_parent_dirs(from_path.parent().ok_or(Error::Unknown)?);

        Ok(())
    }

    fn move_scope(&self, from: &Scope, to: &Scope) -> Result<()> {
        let from_path = from.as_path(&self.root);
        let to_path = to.as_path(&self.root);

        if !to_path.try_exists().unwrap_or_default() {
            fs::create_dir_all(to_path.clone())?;
        }

        fs::rename(from_path.as_path(), to_path.as_path())?;
        remove_empty_parent_dirs(from_path);

        Ok(())
    }

    fn delete(&self, key: &Key) -> Result<()> {
        let path = key.as_path(&self.root);

        fs::remove_file(&path)?;
        remove_empty_parent_dirs(path.parent().ok_or(Error::Unknown)?);

        Ok(())
    }

    fn delete_scope(&self, scope: &Scope) -> Result<()> {
        let path = scope.as_path(&self.root);

        fs::remove_dir_all(&path)?;
        remove_empty_parent_dirs(path);

        Ok(())
    }

    fn clear(&self) -> Result<()> {
        if self.root.exists() {
            let _ = fs::remove_dir_all(&self.root);
        }

        Ok(())
    }
}

impl KeyValueStoreBackend for Disk {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        let lock = FileLock::lock(scope.as_path(&self.root))?;

        let mut store = self.clone();
        callback(&mut store)?;

        lock.unlock()?;

        Ok(())
    }
}

impl Key {
    fn as_path(&self, root: impl AsRef<Path>) -> PathBuf {
        let mut path = root.as_ref().to_path_buf();
        for segment in self.scope() {
            path.push(segment.as_str());
        }
        path.push(format!("{}.json", self.name()));
        path
    }
}

impl Scope {
    fn as_path(&self, root: impl AsRef<Path>) -> PathBuf {
        let mut path = root.as_ref().to_path_buf();
        for segment in self {
            path.push(segment.as_str());
        }
        path
    }
}

trait PathBufExt {
    fn as_key(&self, root: impl AsRef<Path>) -> Result<Key>;

    fn as_scope(&self, root: impl AsRef<Path>) -> Result<Scope>;
}

impl PathBufExt for PathBuf {
    fn as_key(&self, root: impl AsRef<Path>) -> Result<Key> {
        let file_name = self
            .file_name()
            .ok_or(Error::Unknown)?
            .to_string_lossy()
            .to_string();

        let name: SegmentBuf = file_name
            .strip_suffix(".json")
            .map(|s| s.to_string())
            .unwrap_or(file_name)
            .parse()?;

        let scope = self
            .parent()
            .ok_or(Error::Unknown)?
            .to_path_buf()
            .as_scope(root)?;

        Ok(Key::new_scoped(scope, name))
    }

    fn as_scope(&self, root: impl AsRef<Path>) -> Result<Scope> {
        let segments = self
            .strip_prefix(root)
            .map_err(|_| Error::Unknown)?
            .components()
            .map(|component| match component {
                Component::Prefix(_)
                | Component::RootDir
                | Component::CurDir
                | Component::ParentDir => Err(Error::Unknown),
                Component::Normal(segment) => Ok(segment.to_string_lossy().parse()?),
            })
            .collect::<Result<_>>()?;

        Ok(Scope::new(segments))
    }
}

#[derive(Debug)]
struct FileLock {
    file: File,
    lock_path: PathBuf,
}

impl FileLock {
    const POLL_LOCK_INTERVAL: Duration = Duration::from_millis(10);

    pub fn lock(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        if !path.try_exists().unwrap_or_default() {
            fs::create_dir_all(path)?;
        }

        let lock_path = path.join("lockfile.lock");

        let file = loop {
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&lock_path);

            match file {
                Ok(file) => break file,
                _ => thread::sleep(Self::POLL_LOCK_INTERVAL),
            };
        };

        let lock = FileLock { file, lock_path };

        Ok(lock)
    }

    pub fn unlock(&self) -> Result<()> {
        fs::remove_file(&self.lock_path)?;
        Ok(())
    }
}

impl Deref for FileLock {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for FileLock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        self.unlock().ok();
    }
}

fn list_files_recursive(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for result in fs::read_dir(dir)? {
        let path = result?.path();
        if path.is_dir() {
            files.extend(list_files_recursive(path)?);
        } else {
            files.push(path);
        }
    }

    Ok(files)
}

fn list_dirs_recursive(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();

    for result in fs::read_dir(dir)? {
        let path = result?.path();
        if path.is_dir() {
            dirs.extend(list_dirs_recursive(&path)?);
            dirs.push(path);
        }
    }

    Ok(dirs)
}

/// Removes the given directory and all empty parent directories. This function
/// only works on empty directories and will do nothing for files.
fn remove_empty_parent_dirs(path: impl AsRef<Path>) {
    let mut ancestors = path.as_ref().ancestors();
    while ancestors
        .next()
        .and_then(|path| fs::remove_dir(path).ok())
        .is_some()
    {}
}
