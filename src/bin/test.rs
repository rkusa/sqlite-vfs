use std::fs::{File, OpenOptions};

use rusqlite::{Connection, OpenFlags};
use sqlite_vfs::{register, Vfs};

struct TestVfs {}

impl Vfs for TestVfs {
    type File = File;

    fn open(&self, path: &std::path::Path) -> Result<Self::File, std::io::Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(f)
    }
}

fn main() {
    register("test", TestVfs {});

    let conn = Connection::open_with_flags_and_vfs(
        "main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )
    .unwrap();

    let n: i64 = conn.query_row("SELECT 42", [], |row| row.get(0)).unwrap();
    assert_eq!(n, 42)
}
