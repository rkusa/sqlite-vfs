use std::{fs, path::PathBuf, str::FromStr};

use rusqlite::{Connection, OpenFlags};
use sqlite_vfs::{register, OpenAccess, OpenOptions, Vfs};

struct FsVfs;

impl Vfs for FsVfs {
    type File = fs::File;

    fn open(&self, path: &str, opts: OpenOptions) -> Result<Self::File, std::io::Error> {
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
        let f = o.open(path)?;
        Ok(f)
    }

    fn delete(&self, path: &str) -> Result<(), std::io::Error> {
        std::fs::remove_file(path)
    }

    fn exists(&self, path: &str) -> Result<bool, std::io::Error> {
        let path = PathBuf::from_str(path).unwrap();
        Ok(path.is_file())
    }
}

fn main() {
    register("test", FsVfs).unwrap();

    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
        [],
    )
    .unwrap();

    conn.execute("INSERT INTO vals (val) VALUES ('test')", [])
        .unwrap();

    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM vals", [], |row| row.get(0))
        .unwrap();

    println!("Count: {}", n);
}
