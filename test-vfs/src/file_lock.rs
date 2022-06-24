use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::prelude::FromRawFd;

pub use sqlite_vfs::LockKind;

use crate::lock::{flock_exclusive, flock_shared, flock_unlock};

pub struct FileLock {
    file: Option<File>,
    fd: RawFd,
}

impl FileLock {
    pub fn new(file: File) -> io::Result<Self> {
        Ok(Self {
            fd: file.as_raw_fd(),
            file: Some(file),
        })
    }

    pub fn file(&mut self) -> &mut File {
        self.file
            .get_or_insert_with(|| unsafe { File::from_raw_fd(self.fd) })
    }

    pub fn unlock(&self) {
        flock_unlock(self.fd);
    }

    pub fn shared(&self) -> bool {
        flock_shared(self.fd)
    }

    pub fn wait_shared(&self) {
        flock_wait_shared(self.fd)
    }

    pub fn exclusive(&self) -> bool {
        flock_exclusive(self.fd)
    }
}

pub(crate) fn flock_wait_shared(fd: RawFd) {
    unsafe {
        if libc::flock(fd, libc::LOCK_SH) == 0 {
            return;
        }
    }

    let err = std::io::Error::last_os_error();
    panic!("lock shared failed: {}", err);
}

impl Drop for FileLock {
    fn drop(&mut self) {
        self.unlock();
        self.file.take();
    }
}
