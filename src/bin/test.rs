use std::fs::{File, OpenOptions};
use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use sqlite_vfs::{register, Vfs};

struct TestVfs;

impl Vfs for TestVfs {
    type File = File;

    fn open(&self, path: &Path, flags: OpenFlags) -> Result<Self::File, std::io::Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(dbg!(
                flags.contains(OpenFlags::SQLITE_OPEN_READ_WRITE)
                    && !flags.contains(OpenFlags::SQLITE_OPEN_READ_ONLY)
            ))
            .create(dbg!(flags.contains(OpenFlags::SQLITE_OPEN_CREATE)))
            // .truncate(true)
            .open(path)?;
        Ok(f)
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        std::fs::remove_file(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, std::io::Error> {
        Ok(path.is_file())
    }
}

fn main() {
    register("test", TestVfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )
    .unwrap();

    // let journal_mode: String = conn
    //     .query_row("PRAGMA journal_mode=MEMORY", [], |row| row.get(0))
    //     .unwrap();
    // assert_eq!(journal_mode, "memory");

    // let n: i64 = conn.query_row("SELECT 42", [], |row| row.get(0)).unwrap();
    // assert_eq!(n, 42);

    // conn.execute(
    //     "CREATE TABLE vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
    //     [],
    // )
    // .unwrap();

    // conn.execute(
    //     "CREATE TABLE vals2 (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
    //     [],
    // )
    // .unwrap();

    conn.execute("INSERT INTO vals (val) VALUES ('first')", [])
        .unwrap();
}
