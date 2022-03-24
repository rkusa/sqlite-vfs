use std::{
    collections::BTreeMap,
    io::{self, Read, Seek, Write},
    sync::Arc,
};

use log::info;
use parking_lot::Mutex;
use sqlite_vfs::{register, OpenAccess, OpenOptions, Vfs};

#[derive(Debug)]
struct MemVfs {
    files: Arc<parking_lot::Mutex<BTreeMap<String, MemFile>>>,
}

impl MemVfs {
    fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(Default::default())),
        }
    }
}

struct MemFile {
    name: String,
    data: Vec<u8>,
    position: usize,
}

impl std::fmt::Debug for MemFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemFile")
            .field("name", &self.name)
            .field("data", &self.data.len())
            .field("position", &self.position)
            .finish()
    }
}

impl sqlite_vfs::File for MemFile {
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        info!("read_exact {:?} {}", self, buf.len());
        let remaining = self.data.len().saturating_sub(self.position);
        let n = remaining.min(buf.len());
        if n != 0 {
            buf[..n].copy_from_slice(&self.data[self.position..self.position + n]);
            self.position += n;
        }
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        info!("write_all {:?} {}", self, buf.len());
        if self.position > self.data.len() {
            return Err(io::Error::new(io::ErrorKind::Other, ""));
        }
        let current_len = self.data.len();
        let len = buf.len();
        let end = self.position + buf.len();
        self.data.extend((current_len..end).map(|_| 0u8));
        self.data[self.position..end].copy_from_slice(&buf);
        self.position = end;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        info!("flush {:?}", self);
        Ok(())
    }

    fn seek_from_start(&mut self, pos: u64) -> std::io::Result<u64> {
        info!("seek_from_start {:?} {:?}", self, pos);
        self.position = usize::try_from(pos).unwrap();
        Ok(self.position as u64)
    }

    fn file_size(&self) -> Result<u64, std::io::Error> {
        info!("file_size {:?}", self);
        Ok(self.data.len() as u64)
    }

    fn truncate(&mut self, size: u64) -> Result<(), std::io::Error> {
        info!("truncate {:?} {}", self, size);
        let size = usize::try_from(size).unwrap();
        self.data.truncate(size);
        Ok(())
    }
}

impl Vfs for MemVfs {
    type File = MemFile;

    fn open(&self, path: &str, opts: OpenOptions) -> Result<Self::File, std::io::Error> {
        info!("open {:?} {} {:?}", self, path, opts);
        Ok(MemFile {
            name: path.into(),
            data: Default::default(),
            position: 0,
        })
    }

    fn delete(&self, path: &str) -> Result<(), std::io::Error> {
        info!("delete {:?} {}", self, path);
        Ok(())
    }

    fn exists(&self, path: &str) -> Result<bool, std::io::Error> {
        info!("exists {:?} {}", self, path);
        Ok(false)
    }

    /// Check access to `path`. The default implementation always returns `true`.
    fn access(&self, path: &str, write: bool) -> Result<bool, std::io::Error> {
        info!("access {} {}", path, write);
        Ok(true)
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    use rusqlite::{Connection, OpenFlags};
    let vfs = MemVfs::new();

    sqlite_vfs::register("test", vfs).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
        [],
    )?;

    for i in 0..1000 {
        conn.execute("INSERT INTO vals (val) VALUES ('test')", [])?;
    }

    let n: i64 = conn.query_row("SELECT COUNT(*) FROM vals", [], |row| row.get(0))?;

    info!("Count: {}", n);
    conn.cache_flush()?;
    drop(conn);
    Ok(())
}
