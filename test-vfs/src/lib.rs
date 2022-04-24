use std::sync::atomic::{AtomicBool, Ordering};

use sqlite_vfs::{register, RegisterError};

pub mod vfs;

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ERROR: i32 = 1;

static INITIALIZED: AtomicBool = AtomicBool::new(false);

#[no_mangle]
pub extern "C" fn sqlite3_register_test_vfs() -> i32 {
    if !INITIALIZED.swap(true, Ordering::Relaxed) {
        pretty_env_logger::init();
    }

    match register("test-vfs", vfs::FsVfs::default(), true) {
        Ok(_) => SQLITE_OK,
        Err(RegisterError::Nul(_)) => SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    }
}
