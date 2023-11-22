use std::{
    fmt::Display,
    fs,
    fs::{File, OpenOptions},
    ops::{Deref, DerefMut},
    path::{Component, Path, PathBuf},
    thread,
    time::Duration,
};

use serde_json::Value;

use crate::{
    Error, Key, KeyValueStoreBackend, ReadStore, Result, Scope, SegmentBuf, TransactionCallback,
    WriteStore,
};

pub const LOCK_FILE_NAME: &str = "lockfile.lock";
pub const LOCK_FILE_DIR: &str = ".locks";

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Disk {
    root: PathBuf,
    tmp: PathBuf,
}

impl Disk {
    /// This will create a disk based store for the given (base) path and namespace.
    ///
    /// Under the hood this uses two directories: path/namespace and path/tmp.
    /// The latter is used for temporary files for new values for existing keys. Such
    /// values are written first and then renamed (moved) to avoid issues with partially
    /// written files because of I/O issues (disk full) or concurrent reads of the key
    /// as its value is being updated.
    ///
    /// Different instances of this disk based storage that use different namespaces,
    /// but share the same (base) path will all use the same tmp directory. This is
    /// not an issue as the temporary files will have unique names.
    pub fn new(path: &str, namespace: &str) -> Result<Self> {
        let root = PathBuf::from(path).join(namespace);
        let tmp = PathBuf::from(path).join("tmp");

        if !tmp.exists() {
            fs::create_dir_all(&tmp).map_err(|e| {
                Error::IoWithContext(
                    format!("Cannot create directory for tmp files: {}", tmp.display()),
                    e,
                )
            })?;
        }

        Ok(Disk { root, tmp })
    }
}

impl Display for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyValueStore::Disk({})", self.root.display())
    }
}

impl ReadStore for Disk {
    fn is_empty(&self) -> Result<bool> {
        Ok(self
            .root
            .read_dir()
            .map(|mut d| d.next().is_none())
            .unwrap_or(true))
    }

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
            let value =
                fs::read_to_string(key.as_path(&self.root)).map_err(|_| Error::UnknownKey)?;
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

        if key.scope().to_string().starts_with(LOCK_FILE_DIR) {
            return Err(Error::InvalidKey);
        }

        if !dir.try_exists().unwrap_or_default() {
            fs::create_dir_all(dir)?;
        }

        // Always use a tempfile to ensure that the file can be written entirely.
        // If we don't, then we can end up with half-written files in case there
        // is some issue during writing, e.g. disk is full, or the application is
        // suddenly stopped, or the server reboots, etc.

        // tempfile ensures that the temporary file is cleaned up in case it
        // would be left behind because of some issue.
        let tmp_file = tempfile::NamedTempFile::new_in(&self.tmp).map_err(|e| {
            Error::IoWithContext(
                format!(
                    "Issue writing tmp file for key: {}. Check permissions and space on disk.",
                    key
                ),
                e,
            )
        })?;

        fs::write(&tmp_file, format!("{:#}", value).as_bytes()).map_err(|e| {
            Error::IoWithContext(
                format!(
                    "Issue writing tmp file: {} for key: {}. Check permissions and space on disk.",
                    tmp_file.as_ref().display(),
                    key
                ),
                e,
            )
        })?;

        // persist ensures that the temporary file is persisted at the
        // target location and any existing file is replaced. On unix
        // systems this relies on an atomic move.
        tmp_file.persist(&path).map_err(|e| {
            Error::IoWithContext(
                format!(
                    "Cannot rename temp file {} to {}.",
                    e.file.path().display(),
                    path.display()
                ),
                e.error,
            )
        })?;

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

    fn migrate_namespace(&mut self, namespace: kvx_types::NamespaceBuf) -> Result<()> {
        let root_parent = self.root.parent().ok_or(Error::NamespaceMigration(format!(
            "cannot get parent dir for: {}",
            self.root.display()
        )))?;

        let new_root = root_parent.join(namespace.as_str());

        if new_root.exists() {
            // If the target directory already exists, then it must be empty.
            if new_root
                .read_dir()
                .map_err(|e| {
                    Error::NamespaceMigration(format!(
                        "cannot read directory '{}'. Error: {}",
                        new_root.display(),
                        e,
                    ))
                })?
                .next()
                .is_some()
            {
                return Err(Error::NamespaceMigration(format!(
                    "target dir {} already exists and is not empty",
                    new_root.display(),
                )));
            }
        }

        fs::rename(&self.root, &new_root).map_err(|e| {
            Error::NamespaceMigration(format!(
                "cannot rename dir from {} to {}. Error: {}",
                self.root.display(),
                new_root.display(),
                e
            ))
        })?;
        self.root = new_root;
        Ok(())
    }
}

impl KeyValueStoreBackend for Disk {
    fn transaction(&self, scope: &Scope, callback: TransactionCallback) -> Result<()> {
        let lock_file_dir = self.root.join(LOCK_FILE_DIR);

        let _lock = FileLock::lock(scope.as_path(lock_file_dir))?;

        let mut store = self.clone();
        callback(&mut store)?;

        Ok(())
    }
}

trait AsPath {
    fn as_path(&self, root: impl AsRef<Path>) -> PathBuf;
}

impl AsPath for Key {
    fn as_path(&self, root: impl AsRef<Path>) -> PathBuf {
        let mut path = root.as_ref().to_path_buf();
        for segment in self.scope() {
            path.push(segment.as_str());
        }
        path.push(self.name().as_str());
        path
    }
}

impl AsPath for Scope {
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

        let name: SegmentBuf = file_name.parse()?;

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

        let lock_path = path.join(LOCK_FILE_NAME);

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
        if path.is_dir() && !path.ends_with(LOCK_FILE_DIR) && path.read_dir()?.next().is_some() {
            // a non-empty directory exists for the scope, recurse and add
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
