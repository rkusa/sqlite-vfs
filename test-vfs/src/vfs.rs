use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::{fs, io};

use sqlite_vfs::{Lock, OpenAccess, OpenOptions, Vfs};

/// [Vfs] test implementation based on Rust's [std::fs:File]. This implementation is not meant for
/// any use-cases except running SQLite unit tests, as the locking is only managed in process
/// memory.
#[derive(Default)]
pub struct FsVfs {
    state: HashMap<PathBuf, Weak<Mutex<FileState>>>,
}

pub struct File {
    inner: fs::File,
    lock: Lock,
    state: Arc<Mutex<FileState>>,
}

#[derive(Debug, Default)]
struct FileState {
    n_wr_lock: usize,
    n_rd_lock: usize,
}

impl Vfs for FsVfs {
    type File = File;

    fn open(&mut self, path: &Path, opts: OpenOptions) -> Result<Self::File, std::io::Error> {
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

        Ok(File {
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

impl io::Read for File {
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

impl io::Write for File {
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

impl io::Seek for File {
    fn seek(&mut self, from: io::SeekFrom) -> Result<u64, io::Error> {
        self.inner.seek(from)
    }
}

impl sqlite_vfs::File for File {
    fn file_size(&self) -> Result<u64, std::io::Error> {
        Ok(self.inner.metadata()?.len())
    }

    fn truncate(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(size)
    }

    // adapted from memdb: https://github.com/sqlite/sqlite/blob/master/src/memdb.c
    fn lock(&mut self, lock: sqlite_vfs::Lock) -> Result<bool, std::io::Error> {
        let mut state = self.state.lock().unwrap();
        // eprintln!("lock {:?} (from {:?})", lock, self.lock);

        if self.lock == lock {
            return Ok(true);
        }

        match lock {
            Lock::Reserved | Lock::Pending | Lock::Exclusive => {
                if self.lock <= Lock::Shared {
                    if state.n_wr_lock > 0 {
                        return Ok(false);
                    } else {
                        state.n_wr_lock = 1;
                        // eprintln!("n_wr_lock = 1");
                    }
                }
            }
            Lock::Shared => {
                if self.lock > Lock::Shared {
                    assert_eq!(state.n_wr_lock, 1);
                    state.n_wr_lock = 0;

                    // eprintln!("n_wr_lock = 0");
                } else if state.n_wr_lock > 0 {
                    return Ok(false);
                } else {
                    state.n_rd_lock += 1;
                    // eprintln!("n_rd_lock++");
                }
            }
            Lock::None => {
                if self.lock > Lock::Shared {
                    assert_eq!(state.n_wr_lock, 1);
                    state.n_wr_lock = 0;
                    // eprintln!("n_wr_lock = 0");
                }
                assert!(state.n_rd_lock > 0);
                state.n_rd_lock -= 1;
                // eprintln!("n_rd_lock--");
            }
        }

        self.lock = lock;

        Ok(true)
    }

    fn is_reserved(&self) -> Result<bool, std::io::Error> {
        Ok(self.lock >= Lock::Reserved)
    }
}
