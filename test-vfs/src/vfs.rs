use std::borrow::Cow;
use std::fs::{self, File};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::prelude::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use durable_object::client::Client;
use sqlite_vfs::{LockKind, OpenAccess, OpenOptions, Vfs, WalIndex, WalIndexLock};

use crate::lock::Lock;

/// [Vfs] test implementation based on Rust's [std::fs:File]. This implementation is not meant for
/// any use-cases except running SQLite unit tests, as the locking is only managed in process
/// memory.
#[derive(Default)]
pub struct TestVfs {
    temp_counter: AtomicUsize,
}

pub struct Connection {
    client: Mutex<Client>,
    path: PathBuf,
    file: File,
    file_ino: u64,
    lock: Option<Lock>,
}

pub struct WalConnection;

impl Vfs for TestVfs {
    type Handle = Connection;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
        let path = normalize_path(Path::new(&db));
        if path.is_dir() {
            if matches!(opts.access, OpenAccess::Create | OpenAccess::CreateNew) {
                return Err(ErrorKind::PermissionDenied.into());
            }
            return Err(io::Error::new(ErrorKind::Other, "cannot open directory"));
        }

        let client = Mutex::new(Client::connect("/tmp/test-vfs-sock", db)?);

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
        let file = o.open(&path)?;
        let file_ino = file.metadata()?.ino();

        Ok(Connection {
            client,
            path,
            file,
            file_ino,
            // Will be set on first use to reduce unnecessary usage of file descriptors.
            lock: None,
        })
    }

    fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        let path = normalize_path(Path::new(&db));
        fs::remove_file(path)
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        Ok(Path::new(db).is_file())
    }

    fn temporary_name(&self) -> String {
        std::env::temp_dir()
            .join(format!(
                "etilqs_{:x}_{:x}.db",
                std::process::id(),
                self.temp_counter.fetch_add(1, Ordering::AcqRel),
            ))
            .to_string_lossy()
            .to_string()
    }

    fn full_pathname<'a>(&self, db: &'a str) -> Result<Cow<'a, str>, std::io::Error> {
        let path = Path::new(&db);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let path = normalize_path(&path);
        Ok(path
            .to_str()
            .ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::Other,
                    "cannot convert canonicalized path to string",
                )
            })?
            .to_string()
            .into())
    }
}

impl sqlite_vfs::DatabaseHandle for Connection {
    type WalIndex = WalConnection;

    fn size(&self) -> Result<u64, std::io::Error> {
        self.file.metadata().map(|m| m.len())
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error> {
        if data_only {
            self.file.sync_data()
        } else {
            self.file.sync_all()
        }
    }

    fn set_len(&mut self, len: u64) -> Result<(), std::io::Error> {
        self.file.set_len(len)
    }

    fn lock(&mut self, to: LockKind) -> Result<bool, std::io::Error> {
        let lock = match &mut self.lock {
            Some(lock) => lock,
            None => self.lock.get_or_insert(Lock::from_file(&self.file)?),
        };

        // Return false if exclusive was requested and only pending was acquired.
        Ok(lock.lock(to) && lock.current() == to)
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        let lock = match &mut self.lock {
            Some(lock) => lock,
            None => self.lock.get_or_insert(Lock::from_file(&self.file)?),
        };

        Ok(lock.reserved())
    }

    fn current_lock(&self) -> Result<LockKind, std::io::Error> {
        Ok(self
            .lock
            .as_ref()
            .map(|l| l.current())
            .unwrap_or(LockKind::None))
    }

    fn moved(&self) -> Result<bool, std::io::Error> {
        let ino = fs::metadata(&self.path).map(|m| m.ino()).unwrap_or(0);
        Ok(ino == 0 || ino != self.file_ino)
    }
}

impl WalIndex<Connection> for WalConnection {
    fn map(handle: &mut Connection, region: u32) -> Result<[u8; 32768], std::io::Error> {
        let mut client = handle.client.lock().unwrap();
        client.get_wal_index(region)
    }

    fn lock(
        handle: &mut Connection,
        locks: std::ops::Range<u8>,
        lock: WalIndexLock,
    ) -> Result<bool, std::io::Error> {
        let mut client = handle.client.lock().unwrap();
        client.lock_wal_index(
            locks,
            match lock {
                WalIndexLock::None => durable_object::request::WalIndexLock::None,
                WalIndexLock::Shared => durable_object::request::WalIndexLock::Shared,
                WalIndexLock::Exclusive => durable_object::request::WalIndexLock::Exclusive,
            },
        )
    }

    fn delete(handle: &mut Connection) -> Result<(), std::io::Error> {
        let mut client = handle.client.lock().unwrap();
        client.delete_wal_index()?;

        let shm_path = Self::shm_path(handle);
        fs::remove_file(shm_path).ok();

        Ok(())
    }

    fn pull(
        handle: &mut Connection,
        region: u32,
        data: &mut [u8; 32768],
    ) -> Result<(), std::io::Error> {
        // Some tests rely on the existence of the `.db-shm` file, thus make sure it exists,
        // even though it isn't actually used for anything.
        let shm_path = Self::shm_path(handle);
        if !shm_path.is_file() {
            fs::write(shm_path, "")?;
        }

        let mut client = handle.client.lock().unwrap();
        let new_data = client.get_wal_index(region)?;
        data.copy_from_slice(&new_data[..]);
        Ok(())
    }

    fn push(
        handle: &mut Connection,
        region: u32,
        data: &[u8; 32768],
    ) -> Result<(), std::io::Error> {
        let mut client = handle.client.lock().unwrap();
        client.put_wal_index(region, data)?;

        // Some tests rely on the size of the `.db-shm` file, thus make sure it grows
        let shm_path = Self::shm_path(handle);
        let shm = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&shm_path)?;
        let size = region as u64 * 32768;
        if shm.metadata()?.size() < size {
            shm.set_len(size as u64)?;
        }

        Ok(())
    }
}

impl WalConnection {
    fn shm_path(handle: &Connection) -> PathBuf {
        handle.path.with_extension(format!(
            "{}-shm",
            handle
                .path
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("db")
        ))
    }
}

// Source: https://github.com/rust-lang/cargo/blob/7a3b56b4860c0e58dab815549a93198a1c335b64/crates/cargo-util/src/paths.rs#L81
fn normalize_path(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}
