use sqlite_vfs::{register, RegisterError};

pub mod file_lock;
pub mod lock;
pub mod range_lock;
pub mod vfs;

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ERROR: i32 = 1;

#[no_mangle]
pub extern "C" fn sqlite3_register_test_vfs() -> i32 {
    pretty_env_logger::try_init().ok();
    // pretty_env_logger::formatted_builder()
    //     .filter(Some("sqlite_vfs"), log::LevelFilter::Trace)
    //     .try_init()
    //     .ok();

    match register("test-vfs", vfs::TestVfs::default(), true) {
        Ok(_) => SQLITE_OK,
        Err(RegisterError::Nul(_)) => SQLITE_ERROR,
        Err(RegisterError::Register(code)) => code,
    }
}
