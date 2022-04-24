use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use sqlite_vfs::{Lock, OpenAccess, OpenOptions, Vfs};

/// [Vfs] test implementation based on Rust's [std::fs:File]. This implementation is not meant for
/// any use-cases except running SQLite unit tests, as the locking is only managed in process
/// memory.
#[derive(Default)]
pub struct FsVfs {
    state: Mutex<HashMap<PathBuf, Weak<Mutex<FileState>>>>,
    temp_counter: AtomicUsize,
}

pub struct FileHandle {
    #[allow(unused)]
    path: PathBuf,
    inner: File,
    lock: Lock,
    state: Arc<Mutex<FileState>>,
}

#[derive(Debug, Clone, Copy)]
enum FileState {
    /// The file is shared for reading between `count` locks.
    Read { count: usize },
    /// The file has  [Lock::Reserved] lock, so new and existing read locks are still allowed, just
    /// not another [Lock::Reserved] (or write) lock.
    Reserved { count: usize },
    /// The file has a [Lock::Pending] lock, so new read locks are not allowed, and it is awaiting
    /// for the read `count` to get to zero.
    Pending { count: usize },
    /// The file has an [Lock::Exclusive] lock.
    Exclusive,
}

impl Vfs for FsVfs {
    type Handle = FileHandle;

    fn open(&self, path: &Path, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
        let mut o = fs::OpenOptions::new();
        o.read(true).write(opts.access != OpenAccess::Read);
        match opts.access {
            OpenAccess::Create => {
                o.create(true);
            }
            OpenAccess::CreateNew => {
                o.create_new(true);
            }
            _ => {}
        }
        let f = o.open(path)?;

        let mut state = self.state.lock().unwrap();
        let state = if let Some(state) = state.get(path).and_then(|s| s.upgrade()) {
            state
        } else {
            let file_state = Arc::new(Mutex::new(FileState::default()));
            state.insert(path.to_path_buf(), Arc::downgrade(&file_state));
            file_state
        };

        Ok(FileHandle {
            path: path.to_path_buf(),
            inner: f,
            lock: Lock::default(),
            state,
        })
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        std::fs::remove_file(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, std::io::Error> {
        Ok(path.is_file())
    }

    fn temporary_path(&self) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{:x}-{:x}.db",
            std::process::id(),
            self.temp_counter.fetch_add(1, Ordering::AcqRel),
        ))
    }
}

impl io::Read for FileHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.inner.read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.inner.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.inner.read_to_string(buf)
    }
}

impl io::Write for FileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.inner.write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl io::Seek for FileHandle {
    fn seek(&mut self, from: io::SeekFrom) -> Result<u64, io::Error> {
        self.inner.seek(from)
    }
}

impl sqlite_vfs::DatabaseHandle for FileHandle {
    fn file_size(&self) -> Result<u64, std::io::Error> {
        Ok(self.inner.metadata()?.len())
    }

    fn sync(&self, data_only: bool) -> Result<(), std::io::Error> {
        if data_only {
            self.inner.sync_data()
        } else {
            self.inner.sync_all()
        }
    }

    fn truncate(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(size)
    }

    fn lock(&mut self, to: sqlite_vfs::Lock) -> Result<bool, std::io::Error> {
        let mut state = self.state.lock().unwrap();
        // eprintln!("lock {}:", self.path.to_string_lossy());

        // eprintln!("    {:?}: {:?} -> {:?}", state, self.lock, to);

        // If there is already a lock of the requested type, do nothing.
        if self.lock == to {
            return Ok(true);
        }

        let result = match (*state, self.lock, to) {
            // Increment reader count when adding new shared lock.
            (FileState::Read { .. } | FileState::Reserved { .. }, Lock::None, Lock::Shared) => {
                state.increment();
                self.lock = to;
                Ok(true)
            }

            // Don't allow new shared locks when there is a pending or exclusive lock.
            (FileState::Pending { .. } | FileState::Exclusive, Lock::None, Lock::Shared) => {
                Ok(false)
            }

            // Decrement reader count when removing shared lock.
            (
                FileState::Read { .. } | FileState::Reserved { .. } | FileState::Pending { .. },
                Lock::Shared,
                Lock::None,
            ) => {
                state.decrement();
                self.lock = to;
                Ok(true)
            }

            // Issue a reserved lock.
            (FileState::Read { count }, Lock::Shared, Lock::Reserved) => {
                *state = FileState::Reserved { count: count - 1 };
                self.lock = to;
                Ok(true)
            }

            // Return from reserved or pending to shared lock.
            (FileState::Reserved { count }, Lock::Reserved, Lock::Shared)
            | (FileState::Pending { count }, Lock::Pending, Lock::Shared) => {
                *state = FileState::Read { count: count + 1 };
                self.lock = to;
                Ok(true)
            }

            // Return from reserved to none lock.
            (FileState::Reserved { count }, Lock::Reserved, Lock::None) => {
                *state = FileState::Read { count };
                self.lock = to;
                Ok(true)
            }

            // Only a single write lock allowed.
            (
                FileState::Reserved { .. } | FileState::Pending { .. } | FileState::Exclusive,
                Lock::Shared,
                Lock::Reserved,
            ) => Ok(false),

            // Acquire an exclusive lock.
            (FileState::Read { count }, Lock::Shared, Lock::Exclusive)
            | (FileState::Reserved { count }, Lock::Reserved, Lock::Exclusive)
            | (FileState::Pending { count }, Lock::Pending, Lock::Exclusive) => {
                if (matches!(&*state, FileState::Read { .. }) && count == 1) || count == 0 {
                    *state = FileState::Exclusive;
                    self.lock = Lock::Exclusive;
                    Ok(true)
                } else {
                    *state = FileState::Pending { count };
                    self.lock = Lock::Pending;
                    Ok(false)
                }
            }

            // Stop writing.
            (FileState::Exclusive, Lock::Exclusive, Lock::Shared) => {
                *state = FileState::Read { count: 1 };
                self.lock = to;
                Ok(true)
            }
            (FileState::Exclusive, Lock::Exclusive, Lock::None) => {
                *state = FileState::Read { count: 0 };
                self.lock = to;
                Ok(true)
            }

            _ => {
                panic!(
                    "invalid lock transition ({:?}: {:?} to {:?})",
                    state, self.lock, to
                );
                Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "invalid lock transition ({:?}: {:?} to {:?})",
                        state, self.lock, to
                    ),
                ))
            }
        };

        // eprintln!("    {:?}", state);

        result
    }

    fn is_reserved(&self) -> Result<bool, std::io::Error> {
        Ok(matches!(
            &*self.state.lock().unwrap(),
            FileState::Reserved { .. } | FileState::Pending { .. } | FileState::Exclusive
        ))
    }

    fn current_lock(&self) -> Result<Lock, std::io::Error> {
        Ok(self.lock)
    }
}

impl FileState {
    fn increment(&mut self) {
        *self = match *self {
            FileState::Read { count } => FileState::Read { count: count + 1 },
            FileState::Reserved { count } => FileState::Reserved { count: count + 1 },
            FileState::Pending { count } => FileState::Pending { count: count + 1 },
            FileState::Exclusive => FileState::Exclusive,
        };
    }

    fn decrement(&mut self) {
        *self = match *self {
            FileState::Read { count } => FileState::Read { count: count - 1 },
            FileState::Reserved { count } => FileState::Reserved { count: count - 1 },
            FileState::Pending { count } => FileState::Pending { count: count - 1 },
            FileState::Exclusive => FileState::Exclusive,
        };
    }
}

impl Default for FileState {
    fn default() -> Self {
        Self::Read { count: 0 }
    }
}
