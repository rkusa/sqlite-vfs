use std::{collections::BTreeMap, ffi::CStr, sync::Arc};

use log::info;
use parking_lot::Mutex;
use sqlite_vfs::{OpenOptions, Vfs, VfsResult, SQLITE_IOERR};

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
    opts: OpenOptions,
    data: Vec<u8>,
}

impl std::fmt::Debug for MemFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemFile")
            .field("name", &self.name)
            .field("data", &self.data.len())
            .field("kind", &self.opts.kind)
            .finish()
    }
}

impl sqlite_vfs::File for MemFile {
    fn read(&mut self, start: u64, buf: &mut [u8]) -> VfsResult<usize> {
        info!("read {:?} {} {}", self, start, buf.len());
        let start = usize::try_from(start).unwrap();
        let remaining = self.data.len().saturating_sub(start);
        let n = remaining.min(buf.len());
        if n != 0 {
            buf[..n].copy_from_slice(&self.data[start..start + n]);
        }
        Ok(n)
    }

    fn write(&mut self, start: u64, buf: &[u8]) -> VfsResult<usize> {
        info!("write {:?} {} {}", self, start, buf.len());
        let start = usize::try_from(start).unwrap();
        if start > self.data.len() {
            return Err(SQLITE_IOERR);
        }
        let current_len = self.data.len();
        let len = buf.len();
        let end = start + buf.len();
        self.data.extend((current_len..end).map(|_| 0u8));
        self.data[start..end].copy_from_slice(&buf);
        Ok(len)
    }

    fn sync(&mut self) -> VfsResult<()> {
        info!("sync {:?}", self);
        Ok(())
    }

    fn file_size(&self) -> VfsResult<u64> {
        info!("file_size {:?}", self);
        Ok(self.data.len() as u64)
    }

    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        info!("truncate {:?} {}", self, size);
        let size = usize::try_from(size).unwrap();
        self.data.truncate(size);
        Ok(())
    }

    fn sector_size(&self) -> usize {
        1024 * 1024
    }
}

impl Vfs for MemVfs {
    type File = MemFile;

    fn open(&self, path: &CStr, opts: OpenOptions) -> VfsResult<Self::File> {
        let path = path.to_string_lossy();
        info!("open {:?} {} {:?}", self, path, opts);
        Ok(MemFile {
            name: path.into(),
            opts,
            data: Default::default(),
        })
    }

    fn delete(&self, path: &CStr) -> VfsResult<()> {
        let path = path.to_string_lossy();
        info!("delete {:?} {}", self, path);
        Ok(())
    }

    fn exists(&self, path: &CStr) -> VfsResult<bool> {
        let path = path.to_string_lossy();
        info!("exists {:?} {}", self, path);
        Ok(false)
    }

    /// Check access to `path`. The default implementation always returns `true`.
    fn access(&self, path: &CStr, write: bool) -> VfsResult<bool> {
        let path = path.to_string_lossy();
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

    conn.execute_batch(
        r#"
        PRAGMA page_size=32768;
        PRAGMA journal_mode = MEMORY;
        "#,
    )?;

    // uses shm, so not going to work in wasm
    // conn.execute_batch("PRAGMA journal_mode = WAL;")?;

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
