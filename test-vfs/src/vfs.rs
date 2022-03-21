use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use sqlite_vfs::{Lock, OpenAccess, OpenOptions, Vfs};

/// [Vfs] test implementation based on Rust's [std::fs:File]. This implementation is not meant for
/// any use-cases except running SQLite unit tests, as the locking is only managed in process
/// memory.
#[derive(Default)]
pub struct FsVfs {
    state: HashMap<PathBuf, Weak<Mutex<FileState>>>,
}

pub struct FileHandle {
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

    fn open(&mut self, path: &Path, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
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

        let state = if let Some(state) = self.state.get(path).and_then(|s| s.upgrade()) {
            state
        } else {
            let state = Arc::new(Mutex::new(FileState::default()));
            self.state
                .insert(path.to_path_buf(), Arc::downgrade(&state));
            state
        };

        Ok(FileHandle {
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

    fn truncate(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(size)
    }

    fn lock(&mut self, lock: sqlite_vfs::Lock) -> Result<bool, std::io::Error> {
        let mut state = self.state.lock().unwrap();
        // eprintln!("lock {:?} (from {:?})", lock, self.lock);

        if self.lock == lock {
            return Ok(true);
        }

        if !state.transition(self.lock, lock) {
            return Ok(false);
        }

        self.lock = lock;

        Ok(true)
    }

    fn current_lock(&self) -> Result<Lock, std::io::Error> {
        Ok(match &*self.state.lock().unwrap() {
            FileState::Read { count } => {
                if *count == 0 {
                    Lock::None
                } else {
                    Lock::Shared
                }
            }
            FileState::Reserved { .. } => Lock::Reserved,
            FileState::Pending { .. } => Lock::Pending,
            FileState::Exclusive => Lock::Exclusive,
        })
    }
}

impl FileState {
    // It's not pretty but works and is only meant for testing purposes anyway ...
    fn transition(&mut self, from: Lock, to: Lock) -> bool {
        *self = match (*self, from, to) {
            // no change, from and to are the same
            (_, Lock::None, Lock::None)
            | (_, Lock::Shared, Lock::Shared)
            | (_, Lock::Reserved, Lock::Reserved)
            | (_, Lock::Pending, Lock::Pending)
            | (_, Lock::Exclusive, Lock::Exclusive) => return true,

            (FileState::Read { count }, Lock::None, Lock::Shared) => {
                FileState::Read { count: count + 1 }
            }
            (FileState::Read { count }, Lock::None, Lock::Reserved) => {
                FileState::Reserved { count }
            }
            (FileState::Read { count }, Lock::None, Lock::Pending) => FileState::Pending { count },
            (FileState::Read { count }, Lock::None, Lock::Exclusive) => {
                if count == 0 {
                    FileState::Exclusive
                } else {
                    return false;
                }
            }

            (FileState::Read { count }, Lock::Shared, Lock::None) => {
                FileState::Read { count: count - 1 }
            }
            (FileState::Read { count }, Lock::Shared, Lock::Reserved) => {
                FileState::Reserved { count: count - 1 }
            }
            (FileState::Read { count }, Lock::Shared, Lock::Pending) => {
                FileState::Pending { count: count - 1 }
            }
            (FileState::Read { count }, Lock::Shared, Lock::Exclusive) => {
                if count == 1 {
                    FileState::Exclusive
                } else {
                    return false;
                }
            }

            // transition from reserved lock
            (FileState::Reserved { count }, Lock::None, Lock::Shared) => {
                FileState::Reserved { count: count + 1 }
            }
            (
                FileState::Reserved { .. },
                Lock::None | Lock::Shared,
                Lock::Reserved | Lock::Pending | Lock::Exclusive,
            ) => return false,

            (FileState::Reserved { count }, Lock::Shared, Lock::None) => {
                FileState::Reserved { count: count - 1 }
            }
            (FileState::Reserved { count }, Lock::Reserved, Lock::None) => {
                FileState::Read { count }
            }
            (FileState::Reserved { count }, Lock::Reserved, Lock::Shared) => {
                FileState::Read { count: count + 1 }
            }
            (FileState::Reserved { count }, Lock::Reserved, Lock::Pending) => {
                FileState::Pending { count }
            }
            (FileState::Reserved { count }, Lock::Reserved, Lock::Exclusive) => {
                if count == 0 {
                    FileState::Exclusive
                } else {
                    return false;
                }
            }

            // transition from pending lock
            (FileState::Pending { count }, Lock::Pending, Lock::None) => FileState::Read { count },
            (FileState::Pending { count }, Lock::Pending, Lock::Shared) => {
                FileState::Read { count: count + 1 }
            }
            (FileState::Pending { count }, Lock::Pending, Lock::Reserved) => {
                FileState::Reserved { count }
            }
            (FileState::Pending { count }, Lock::Pending, Lock::Exclusive) => {
                if count == 0 {
                    FileState::Exclusive
                } else {
                    return false;
                }
            }

            // transition from exclusive lock
            (FileState::Exclusive, Lock::Exclusive, Lock::None) => FileState::Read { count: 0 },
            (FileState::Exclusive, Lock::Exclusive, Lock::Shared) => FileState::Read { count: 1 },
            (FileState::Exclusive, Lock::Exclusive, Lock::Reserved) => {
                FileState::Reserved { count: 0 }
            }
            (FileState::Exclusive, Lock::Exclusive, Lock::Pending) => {
                FileState::Pending { count: 0 }
            }

            // drain readers while in pending state
            (FileState::Pending { count }, Lock::Shared, Lock::None) => {
                FileState::Pending { count: count - 1 }
            }

            // no new locks allowed while in Pending or Exclusive
            (FileState::Pending { .. }, Lock::None | Lock::Shared, _) => return false,
            (FileState::Exclusive, Lock::None, _) => return false,

            // invalid state and from lock combination
            (FileState::Read { .. }, Lock::Reserved | Lock::Pending | Lock::Exclusive, _)
            | (FileState::Reserved { .. }, Lock::Pending | Lock::Exclusive, _)
            | (FileState::Pending { .. }, Lock::Reserved | Lock::Exclusive, _)
            | (FileState::Exclusive, Lock::Shared | Lock::Reserved | Lock::Pending, _) => {
                unreachable!("state does not match current lock")
            }
        };

        true
    }
}

impl Default for FileState {
    fn default() -> Self {
        Self::Read { count: 0 }
    }
}
