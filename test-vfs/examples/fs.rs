use rusqlite::{Connection, OpenFlags};
use sqlite_vfs::register;
use test_vfs::vfs::FsVfs;

fn main() {
    register("test", FsVfs::default(), false).unwrap();

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
