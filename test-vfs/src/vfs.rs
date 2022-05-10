use std::io::{ErrorKind, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use durable_object::client::Client;
use sqlite_vfs::{Lock, OpenAccess, OpenOptions, Vfs};

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
}

impl sqlite_vfs::DatabaseHandle for Connection {
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
