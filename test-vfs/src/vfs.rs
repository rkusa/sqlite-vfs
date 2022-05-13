use std::borrow::Cow;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use durable_object::client::Client;
use sqlite_vfs::{Lock, OpenAccess, OpenOptions, Vfs, WalIndex, WalIndexLock};

/// [Vfs] test implementation based on Rust's [std::fs:File]. This implementation is not meant for
/// any use-cases except running SQLite unit tests, as the locking is only managed in process
/// memory.
#[derive(Default)]
pub struct TestVfs {
    temp_counter: AtomicUsize,
}

pub struct Connection {
    client: Mutex<Client>,
    lock: Lock,
}

pub struct WalConnection;

impl Vfs for TestVfs {
    type Handle = Connection;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
        // TODO: open options
        Ok(Connection {
            client: Mutex::new(Client::connect(
                "127.0.0.1:6000",
                db,
                match opts.access {
                    OpenAccess::Read => durable_object::request::OpenAccess::Read,
                    OpenAccess::Write => durable_object::request::OpenAccess::Write,
                    OpenAccess::Create => durable_object::request::OpenAccess::Create,
                    OpenAccess::CreateNew => durable_object::request::OpenAccess::CreateNew,
                },
            )?),
            lock: Lock::default(),
        })
    }

    fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        Client::delete("127.0.0.1:6000", db)
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        Client::exists("127.0.0.1:6000", db)
    }

    fn temporary_name(&self) -> String {
        std::env::temp_dir()
            .join(format!(
                "{:x}-{:x}.db",
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
                    "cannot convert canoicalized path to string",
                )
            })?
            .to_string()
            .into())
    }
}

impl sqlite_vfs::DatabaseHandle for Connection {
    type WalIndex = WalConnection;

    fn size(&self) -> Result<u64, std::io::Error> {
        let mut client = self.client.lock().unwrap();
        client.size()
    }

    fn read_exact_at(&self, mut buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        let mut client = self.client.lock().unwrap();
        let data = client.get(offset..(offset + buf.len() as u64))?;
        buf.write_all(data)?;
        if data.len() < buf.len() {
            return Err(ErrorKind::UnexpectedEof.into());
        }

        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        let mut client = self.client.lock().unwrap();
        client.put(offset, buf)
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<(), std::io::Error> {
        let mut client = self.client.lock().unwrap();
        client.truncate(size)
    }

    fn lock(&mut self, to: sqlite_vfs::Lock) -> Result<bool, std::io::Error> {
        // eprintln!("lock {}:", self.path.to_string_lossy());

        // eprintln!("    {:?}: {:?} -> {:?}", state, self.lock, to);

        // If there is already a lock of the requested type, do nothing.
        if self.lock == to {
            return Ok(true);
        }

        let mut client = self.client.lock().unwrap();
        let lock = client.lock(match to {
            Lock::None => durable_object::request::Lock::None,
            Lock::Shared => durable_object::request::Lock::Shared,
            Lock::Reserved => durable_object::request::Lock::Reserved,
            Lock::Pending => durable_object::request::Lock::Pending,
            Lock::Exclusive => durable_object::request::Lock::Exclusive,
        })?;
        if let Some(lock) = lock {
            self.lock = match lock {
                durable_object::request::Lock::None => Lock::None,
                durable_object::request::Lock::Shared => Lock::Shared,
                durable_object::request::Lock::Reserved => Lock::Reserved,
                durable_object::request::Lock::Pending => Lock::Pending,
                durable_object::request::Lock::Exclusive => Lock::Exclusive,
            };
            Ok(self.lock == to)
        } else {
            Ok(false)
        }
    }

    fn is_reserved(&self) -> Result<bool, std::io::Error> {
        if self.lock > Lock::Shared {
            return Ok(true);
        }

        let mut client = self.client.lock().unwrap();
        client.is_reserved()
    }

    fn current_lock(&self) -> Result<Lock, std::io::Error> {
        Ok(self.lock)
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
        client.delete_wal_index()
    }

    fn pull(
        handle: &mut Connection,
        region: u32,
        data: &mut [u8; 32768],
    ) -> Result<(), std::io::Error> {
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
        client.put_wal_index(region, data)
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
