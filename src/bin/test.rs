use std::fs::{File, OpenOptions};

use rusqlite::{Connection, OpenFlags};
use sqlite_vfs::{register, Vfs};

struct TestVfs;

impl Vfs for TestVfs {
    type File = File;

    fn open(&self, path: &std::path::Path) -> Result<Self::File, std::io::Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(f)
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), std::io::Error> {
        std::fs::remove_file(path)
    }
}

fn main() {
    register("test", TestVfs);

    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )
    .unwrap();

    let n: i64 = conn.query_row("SELECT 42", [], |row| row.get(0)).unwrap();
    assert_eq!(n, 42);

    conn.execute(
        "CREATE TABLE vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
        [],
    )
    .unwrap();
}
