use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use sqlite_vfs::{register, Vfs};

struct BlockVfs<const BLOCK_SIZE: u32> {}

impl<const BLOCK_SIZE: u32> Vfs for BlockVfs<BLOCK_SIZE> {
    type File = Blocks<BLOCK_SIZE>;

    fn open(
        &self,
        path: &std::path::Path,
        _flags: OpenFlags,
    ) -> Result<Self::File, std::io::Error> {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let count = File::open(format!("db/{}.0.block", name))
            .and_then(|mut f| {
                f.seek(SeekFrom::Start(28))?;
                let mut bytes = [0u8; 4];
                f.read_exact(&mut bytes[..])?;

                Ok(u32::from_be_bytes(bytes))
            })
            .unwrap_or(0);
        Ok(Blocks {
            name,
            count,
            offset: 0,
            blocks: Default::default(),
        })
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        log::trace!("Ignore delete {}", path.to_string_lossy());
        // std::fs::remove_file(path)
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, std::io::Error> {
        let path = if let Some(ext) = path.extension() {
            path.with_extension(format!("{}.0.block", ext.to_string_lossy()))
        } else {
            path.with_extension("0.block")
        };
        Ok(dbg!(path.is_file()))
    }
}

struct Block {
    file: File,
    dirty: bool,
}

struct Blocks<const BLOCK_SIZE: u32> {
    name: String,
    count: u32,
    offset: u64,
    blocks: HashMap<usize, Block>,
}

impl<const BLOCK_SIZE: u32> sqlite_vfs::File for Blocks<BLOCK_SIZE> {
    fn file_size(&self) -> Result<u64, std::io::Error> {
        Ok(dbg!((self.count * BLOCK_SIZE) as u64))
    }
}

impl<const BLOCK_SIZE: u32> Seek for Blocks<BLOCK_SIZE> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let offset = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::End(_) => unimplemented!(),
            SeekFrom::Current(_) => unimplemented!(),
        };

        log::trace!("seek to {}", offset);

        self.offset = offset;
        let block = self.current()?;
        block
            .file
            .seek(SeekFrom::Start(offset % BLOCK_SIZE as u64))?;

        Ok(self.offset)
    }
}

impl<const BLOCK_SIZE: u32> Read for Blocks<BLOCK_SIZE> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.current()?.file.read(buf)?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl<const BLOCK_SIZE: u32> Write for Blocks<BLOCK_SIZE> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let block = self.current()?;
        let n = block.file.write(buf)?;
        block.dirty = true;
        self.offset += n as u64;

        let count = ((self.offset / BLOCK_SIZE as u64) + 1) as u32;
        if count > self.count {
            self.count = count;
        }

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        for block in self.blocks.values_mut() {
            if block.dirty {
                block.file.flush()?;
                block.dirty = false;
            }
        }
        Ok(())
    }
}

impl<const BLOCK_SIZE: u32> Blocks<BLOCK_SIZE> {
    fn current(&mut self) -> Result<&mut Block, std::io::Error> {
        let index: usize = (self.offset / BLOCK_SIZE as u64) as usize;

        if let Entry::Vacant(entry) = self.blocks.entry(index) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(format!("db/{}.{}.block", self.name, index))?;
            entry.insert(Block { file, dirty: false });
        }

        log::trace!("Block: {}", index);

        Ok(self.blocks.get_mut(&index).unwrap())
    }
}

fn main() {
    register("test", BlockVfs::<4096> {}).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )
    .unwrap();

    conn.execute("PRAGMA page_size = 4096;", []).unwrap();
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode=MEMORY", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_mode, "memory");

    let first: String = conn
        .query_row("SELECT val FROM vals", [], |row| row.get(0))
        .unwrap();
    assert_eq!(first, "first");

    // let n: i64 = conn.query_row("SELECT 42", [], |row| row.get(0)).unwrap();
    // assert_eq!(n, 42);

    // conn.execute(
    //     "CREATE TABLE vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
    //     [],
    // )
    // .unwrap();

    // conn.execute("INSERT INTO vals (val) VALUES ('first')", [])
    //     .unwrap();
}
