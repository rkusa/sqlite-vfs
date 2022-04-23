#![allow(clippy::question_mark)]
//! Create a custom SQLite virtual file system by implementing the [Vfs] trait and registering it
//! using [register].

use std::ffi::{c_void, CStr, CString};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, ManuallyDrop, MaybeUninit};
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::slice;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::time::Instant;

mod ffi;

/// A file opened by [Vfs].
pub trait DatabaseHandle: Read + Seek + Write {
    /// Return the current file-size of the database.
    fn file_size(&self) -> Result<u64, std::io::Error>;

    /// Make sure all writes are committed to the underlying storage. If `data_only` is set to
    /// `true`, only the data and not the metadata (like size, access time, etc) should be synced.
    fn sync(&self, data_only: bool) -> Result<(), std::io::Error>;

    /// Truncat the database file to the specified `size`.
    fn truncate(&mut self, size: u64) -> Result<(), std::io::Error>;

    /// Lock the database. Returns whether the requested lock could be aquired.
    /// Locking sequence:
    /// - The lock is nevered moved from [Lock::None] to anything higher than [Lock::Shared].
    /// - A [Lock::Pending] is never requested explicitly.
    /// - A [Lock::Shared] is always held when a [Lock::Reserved] lock is requested
    fn lock(&mut self, lock: Lock) -> Result<bool, std::io::Error>;

    /// Unlock the database.
    fn unlock(&mut self, lock: Lock) -> Result<bool, std::io::Error> {
        self.lock(lock)
    }

    /// Check if the database this handle points to holds a [Lock::Reserved], [Lock::Pending] or
    /// [Lock::Exclusive] lock.
    fn is_reserved(&self) -> Result<bool, std::io::Error>;

    /// Return the current [Lock] of the this handle.
    fn current_lock(&self) -> Result<Lock, std::io::Error>;

    /// Change the chunk size of the database to `chunk_size`.
    fn set_chunk_size(&self, _chunk_size: usize) -> Result<(), std::io::Error> {
        Ok(())
    }
}

/// A virtual file system for SQLite.
pub trait Vfs {
    /// The file returned by [Vfs::open].
    type Handle: DatabaseHandle;

    /// Open the database (of type `opts.kind`) at `path`.
    fn open(&mut self, path: &Path, opts: OpenOptions) -> Result<Self::Handle, std::io::Error>;

    /// Delete the database at `path`.
    fn delete(&self, path: &Path) -> Result<(), std::io::Error>;

    /// Check if a database at `path` already exists.
    fn exists(&self, path: &Path) -> Result<bool, std::io::Error>;

    /// Generate and return a path for a temporary database.
    fn temporary_path(&self) -> PathBuf;

    /// Check access to `path`. The default implementation always returns `true`.
    fn access(&self, _path: &Path, _write: bool) -> Result<bool, std::io::Error> {
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenOptions {
    /// The object type that is being opened.
    pub kind: OpenKind,

    /// The access an object is opened with.
    pub access: OpenAccess,

    /// The file should be deleted when it is closed.
    delete_on_close: bool,
}

/// The object type that is being opened.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenKind {
    MainDb,
    MainJournal,
    TempDb,
    TempJournal,
    TransientDb,
    SubJournal,
    SuperJournal,
    Wal,
}

/// The access an object is opened with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenAccess {
    /// Read access.
    Read,

    /// Write access (includes read access).
    Write,

    /// Create the file if it does not exist (includes write and read access).
    Create,

    /// Create the file, but throw if it it already exist (includes write and read access).
    CreateNew,
}

/// The access an object is opened with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Lock {
    /// No locks are held. The database may be neither read nor written. Any internally cached data
    /// is considered suspect and subject to verification against the database file before being
    /// used. Other processes can read or write the database as their own locking states permit.
    /// This is the default state.
    None,

    /// The database may be read but not written. Any number of processes can hold [Lock::Shared]
    /// locks at the same time, hence there can be many simultaneous readers. But no other thread or
    /// process is allowed to write to the database file while one or more [Lock::Shared] locks are
    /// active.
    Shared,

    /// A [Lock::Reserved] lock means that the process is planning on writing to the database file
    /// at some point in the future but that it is currently just reading from the file. Only a
    /// single [Lock::Reserved] lock may be active at one time, though multiple [Lock::Shared] locks
    /// can coexist with a single [Lock::Reserved] lock. [Lock::Reserved] differs from
    /// [Lock::Pending] in that new [Lock::Shared] locks can be acquired while there is a
    /// [Lock::Reserved] lock.
    Reserved,

    /// A [Lock::Pending] lock means that the process holding the lock wants to write to the
    /// database as soon as possible and is just waiting on all current [Lock::Shared] locks to
    /// clear so that it can get an [Lock::Exclusive] lock. No new [Lock::Shared] locks are
    /// permitted against the database if a [Lock::Pending] lock is active, though existing
    /// [Lock::Shared] locks are allowed to continue.
    Pending,

    /// An [Lock::Exclusive] lock is needed in order to write to the database file. Only one
    /// [Lock::Exclusive] lock is allowed on the file and no other locks of any kind are allowed to
    /// coexist with an [Lock::Exclusive] lock. In order to maximize concurrency, SQLite works to
    /// minimize the amount of time that [Lock::Exclusive] locks are held.
    Exclusive,
}

struct State<V> {
    name: CString,
    vfs: V,
    io_methods: ffi::sqlite3_io_methods,
    last_error: Arc<Mutex<Option<(i32, std::io::Error)>>>,
}

/// Register a virtual file system ([Vfs]) to SQLite.
pub fn register<F: DatabaseHandle, V: Vfs<Handle = F>>(
    name: &str,
    vfs: V,
    as_default: bool,
) -> Result<(), RegisterError> {
    let io_methods = ffi::sqlite3_io_methods {
        iVersion: 3,
        xClose: Some(io::close::<V, F>),
        xRead: Some(io::read::<F>),
        xWrite: Some(io::write::<F>),
        xTruncate: Some(io::truncate::<F>),
        xSync: Some(io::sync::<F>),
        xFileSize: Some(io::file_size::<F>),
        xLock: Some(io::lock::<F>),
        xUnlock: Some(io::unlock::<F>),
        xCheckReservedLock: Some(io::check_reserved_lock::<F>),
        xFileControl: Some(io::file_control::<F>),
        xSectorSize: Some(io::sector_size::<F>),
        xDeviceCharacteristics: Some(io::device_characteristics::<F>),
        xShmMap: Some(io::shm_map::<F>),
        xShmLock: Some(io::shm_lock::<F>),
        xShmBarrier: Some(io::shm_barrier),
        xShmUnmap: Some(io::shm_unmap::<F>),
        xFetch: Some(io::mem_fetch::<F>),
        xUnfetch: Some(io::mem_unfetch::<F>),
    };
    let name = CString::new(name)?;
    let name_ptr = name.as_ptr();
    let ptr = Box::into_raw(Box::new(State {
        name,
        vfs,
        io_methods,
        last_error: Default::default(),
    }));
    let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        iVersion: 3,
        szOsFile: size_of::<FileState<F>>() as i32,
        mxPathname: MAX_PATH_LENGTH as i32, // max path length supported by VFS
        pNext: null_mut(),
        zName: name_ptr,
        pAppData: ptr as _,
        xOpen: Some(vfs::open::<F, V>),
        xDelete: Some(vfs::delete::<V>),
        xAccess: Some(vfs::access::<V>),
        xFullPathname: Some(vfs::full_pathname::<V>),
        xDlOpen: Some(vfs::dlopen),
        xDlError: Some(vfs::dlerror),
        xDlSym: Some(vfs::dlsym),
        xDlClose: Some(vfs::dlclose),
        xRandomness: Some(vfs::randomness),
        xSleep: Some(vfs::sleep),
        xCurrentTime: Some(vfs::current_time::<V>),
        xGetLastError: Some(vfs::get_last_error::<V>),
        xCurrentTimeInt64: Some(vfs::current_time_int64::<V>),
        xSetSystemCall: None,
        xGetSystemCall: None,
        xNextSystemCall: None,
    }));

    let result = unsafe { ffi::sqlite3_vfs_register(vfs, as_default as i32) };
    if result != ffi::SQLITE_OK {
        return Err(RegisterError::Register(result));
    }

    // TODO: return object that allows to unregister (and cleanup the memory)?

    Ok(())
}

// TODO: add to [Vfs]?
const MAX_PATH_LENGTH: usize = 512;

#[repr(C)]
struct FileState<F> {
    base: ffi::sqlite3_file,
    vfs: *mut ffi::sqlite3_vfs,
    ext: MaybeUninit<FileExt<F>>,
}

#[repr(C)]
struct FileExt<F> {
    vfs_name: CString,
    path: PathBuf,
    file: F,
    delete_on_close: bool,
    last_error: Arc<Mutex<Option<(i32, std::io::Error)>>>,
}

// Example mem-fs implementation:
// https://github.com/sqlite/sqlite/blob/a959bf53110bfada67a3a52187acd57aa2f34e19/ext/misc/memvfs.c
mod vfs {
    use super::*;

    /// Open a new file handler.
    pub unsafe extern "C" fn open<F: DatabaseHandle, V: Vfs<Handle = F>>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        p_file: *mut ffi::sqlite3_file,
        flags: c_int,
        p_out_flags: *mut c_int,
    ) -> c_int {
        // TODO: any way to use OsStr instead?
        let name =
            (!z_name.is_null()).then(|| CStr::from_ptr(z_name).to_string_lossy().to_string());
        log::trace!("open z_name={:?} flags={}", name, flags);

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };

        let mut opts = match OpenOptions::from_flags(flags) {
            Some(opts) => opts,
            None => {
                return state.set_last_error(
                    ffi::SQLITE_CANTOPEN,
                    std::io::Error::new(std::io::ErrorKind::Other, "invalid open flags"),
                );
            }
        };

        if z_name.is_null() && !opts.delete_on_close {
            return state.set_last_error(
                ffi::SQLITE_CANTOPEN,
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "delete on close expected for temporary database",
                ),
            );
        }

        let out_file = match (p_file as *mut FileState<F>).as_mut() {
            Some(f) => f,
            None => {
                return state.set_last_error(
                    ffi::SQLITE_CANTOPEN,
                    std::io::Error::new(std::io::ErrorKind::Other, "invalid file pointer"),
                );
            }
        };

        let path = name.map_or_else(|| state.vfs.temporary_path(), PathBuf::from);
        let file = match state.vfs.open(&path, opts.clone()) {
            Ok(f) => f,
            Err(err) => {
                if err.kind() == ErrorKind::PermissionDenied && opts.access != OpenAccess::Read {
                    // Try again as readonly
                    opts.access = OpenAccess::Read;
                    if let Ok(f) = state.vfs.open(&path, opts.clone()) {
                        f
                    } else {
                        return state.set_last_error(ffi::SQLITE_CANTOPEN, err);
                    }
                } else {
                    return state.set_last_error(ffi::SQLITE_CANTOPEN, err);
                }
            }
        };

        if let Some(p_out_flags) = p_out_flags.as_mut() {
            *p_out_flags = opts.to_flags();
        }

        out_file.base.pMethods = &state.io_methods;
        out_file.vfs = p_vfs;
        out_file.ext.write(FileExt {
            vfs_name: state.name.clone(),
            path,
            file,
            delete_on_close: opts.delete_on_close,
            last_error: Arc::clone(&state.last_error),
        });

        ffi::SQLITE_OK
    }

    /// Delete the file located at `z_path`. If the `sync_dir` argument is true, ensure the
    /// file-system modifications are synced to disk before returning.
    pub unsafe extern "C" fn delete<V: Vfs>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        _sync_dir: c_int,
    ) -> c_int {
        let name = if z_path.is_null() {
            None
        } else {
            CStr::from_ptr(z_path).to_str().ok()
        };
        log::trace!("delete z_name={:?}", name);

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_DELETE,
        };

        let path = CStr::from_ptr(z_path);
        // TODO: any way to use OsStr instead?
        let path = path.to_string_lossy().to_string();

        match state.vfs.delete(path.as_ref()) {
            Ok(_) => ffi::SQLITE_OK,
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    ffi::SQLITE_OK
                } else {
                    state.set_last_error(ffi::SQLITE_DELETE, err)
                }
            }
        }
    }

    /// Test for access permissions. Return true if the requested permission is available, or false
    /// otherwise.
    pub unsafe extern "C" fn access<V: Vfs>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        flags: c_int,
        p_res_out: *mut c_int,
    ) -> c_int {
        let name = if z_path.is_null() {
            None
        } else {
            CStr::from_ptr(z_path).to_str().ok()
        };
        log::trace!("access z_name={:?} flags={}", name, flags);

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };

        let path = CStr::from_ptr(z_path);
        // TODO: any way to use OsStr instead?
        let path = path.to_string_lossy().to_string();

        let result = match flags {
            ffi::SQLITE_ACCESS_EXISTS => state.vfs.exists(path.as_ref()),
            ffi::SQLITE_ACCESS_READ => state.vfs.access(path.as_ref(), false),
            ffi::SQLITE_ACCESS_READWRITE => state.vfs.access(path.as_ref(), true),
            _ => return ffi::SQLITE_IOERR_ACCESS,
        };

        if let Err(err) = result.and_then(|ok| {
            let p_res_out: &mut c_int = p_res_out.as_mut().ok_or_else(null_ptr_error)?;
            *p_res_out = ok as i32;
            Ok(())
        }) {
            return state.set_last_error(ffi::SQLITE_IOERR_ACCESS, err);
        }

        ffi::SQLITE_OK
    }

    /// Populate buffer `z_out` with the full canonical pathname corresponding to the pathname in
    /// `z_path`. `z_out` is guaranteed to point to a buffer of at least (INST_MAX_PATHNAME+1)
    /// bytes.
    pub unsafe extern "C" fn full_pathname<V>(
        _p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        n_out: c_int,
        z_out: *mut c_char,
    ) -> c_int {
        let name = CStr::from_ptr(z_path);
        log::trace!("full_pathname name={}", name.to_string_lossy());

        let name = name.to_bytes_with_nul();
        if name.len() > n_out as usize || name.len() > MAX_PATH_LENGTH {
            return ffi::SQLITE_ERROR;
        }
        let out = slice::from_raw_parts_mut(z_out as *mut u8, name.len());
        out.copy_from_slice(name);

        ffi::SQLITE_OK
    }

    /// Open the dynamic library located at `z_path` and return a handle.
    pub unsafe extern "C" fn dlopen(
        _p_vfs: *mut ffi::sqlite3_vfs,
        _z_path: *const c_char,
    ) -> *mut c_void {
        log::trace!("dlopen");

        null_mut()
    }

    /// Populate the buffer `z_err_msg` (size `n_byte` bytes) with a human readable utf-8 string
    /// describing the most recent error encountered associated with dynamic libraries.
    pub unsafe extern "C" fn dlerror(
        _p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) {
        log::trace!("dlerror");

        let msg = concat!("Loadable extensions are not supported", "\0");
        ffi::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr() as _);
    }

    /// Return a pointer to the symbol `z_sym` in the dynamic library pHandle.
    pub unsafe extern "C" fn dlsym(
        _p_vfs: *mut ffi::sqlite3_vfs,
        _p: *mut c_void,
        _z_sym: *const c_char,
    ) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
        log::trace!("dlsym");

        None
    }

    /// Close the dynamic library handle `p_handle`.
    pub unsafe extern "C" fn dlclose(_p_vfs: *mut ffi::sqlite3_vfs, _p_handle: *mut c_void) {
        log::trace!("dlclose");
    }

    /// Populate the buffer pointed to by `z_buf_out` with `n_byte` bytes of random data.
    pub unsafe extern "C" fn randomness(
        _p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_buf_out: *mut c_char,
    ) -> c_int {
        log::trace!("randomness");

        use rand::Rng;

        let bytes = slice::from_raw_parts_mut(z_buf_out, n_byte as usize);
        rand::thread_rng().fill(bytes);
        bytes.len() as c_int
    }

    /// Sleep for `n_micro` microseconds. Return the number of microseconds actually slept.
    pub unsafe extern "C" fn sleep(_p_vfs: *mut ffi::sqlite3_vfs, n_micro: c_int) -> c_int {
        log::trace!("sleep");

        let instant = Instant::now();
        thread::sleep(Duration::from_micros(n_micro as u64));
        instant.elapsed().as_micros() as c_int
    }

    /// Return the current time as a Julian Day number in `p_time_out`.
    pub unsafe extern "C" fn current_time<V>(
        _p_vfs: *mut ffi::sqlite3_vfs,
        p_time_out: *mut f64,
    ) -> c_int {
        log::trace!("current_time");

        let now = time::OffsetDateTime::now_utc().unix_timestamp() as f64;
        *p_time_out = 2440587.5 + now / 864.0e5;
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn get_last_error<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) -> c_int {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        if let Some((_, err)) = state.last_error.lock().unwrap().as_ref() {
            let msg = match CString::new(err.to_string()) {
                Ok(msg) => msg,
                Err(_) => return ffi::SQLITE_ERROR,
            };

            let msg = msg.to_bytes_with_nul();
            if msg.len() > n_byte as usize {
                return ffi::SQLITE_ERROR;
            }
            let out = slice::from_raw_parts_mut(z_err_msg as *mut u8, msg.len());
            out.copy_from_slice(msg);
        }
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn current_time_int64<V>(
        _p_vfs: *mut ffi::sqlite3_vfs,
        p: *mut i64,
    ) -> i32 {
        log::trace!("current_time_int64");

        let now = time::OffsetDateTime::now_utc().unix_timestamp() as f64;
        *p = ((2440587.5 + now / 864.0e5) * 864.0e5) as i64;
        ffi::SQLITE_OK
    }
}

mod io {
    use std::mem;

    use super::*;

    /// Close a file.
    pub unsafe extern "C" fn close<V: Vfs, F>(p_file: *mut ffi::sqlite3_file) -> c_int {
        log::trace!("close");

        if let Some(f) = (p_file as *mut FileState<F>).as_mut() {
            let ext = f.ext.assume_init_ref();
            if ext.delete_on_close {
                if let Ok(state) = vfs_state::<V>(f.vfs) {
                    if let Err(err) = state.vfs.delete(&ext.path) {
                        return state.set_last_error(ffi::SQLITE_DELETE, err);
                    }
                }
            }

            let ext = mem::replace(&mut f.ext, MaybeUninit::uninit());
            let ext = ext.assume_init(); // extract the value to drop it
            log::trace!("close ({:?})", ext.path);
        }

        ffi::SQLITE_OK
    }

    /// Read data from a file.
    pub unsafe extern "C" fn read<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        z_buf: *mut c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        log::trace!("read offset={} len={}", i_ofst, i_amt);

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CLOSE,
        };
        log::trace!("read ({:?})", state.path);

        match state.file.seek(SeekFrom::Start(i_ofst as u64)) {
            Ok(o) => {
                if o != i_ofst as u64 {
                    return ffi::SQLITE_IOERR_READ;
                }
            }
            Err(err) => {
                return state.set_last_error(ffi::SQLITE_IOERR_READ, err);
            }
        }

        let out = slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize);
        if let Err(err) = state.file.read_exact(out) {
            let kind = err.kind();
            if kind == ErrorKind::UnexpectedEof {
                return ffi::SQLITE_IOERR_SHORT_READ;
            } else {
                return state.set_last_error(ffi::SQLITE_IOERR_READ, err);
            }
        }

        ffi::SQLITE_OK
    }

    /// Write data to a file.
    pub unsafe extern "C" fn write<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        z: *const c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        log::trace!("write offset={} len={}", i_ofst, i_amt);

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_WRITE,
        };
        log::trace!("write ({:?})", state.path);

        match state.file.seek(SeekFrom::Start(i_ofst as u64)) {
            Ok(o) => {
                if o != i_ofst as u64 {
                    return ffi::SQLITE_IOERR_WRITE;
                }
            }
            Err(err) => {
                return state.set_last_error(ffi::SQLITE_IOERR_WRITE, err);
            }
        }

        let data = slice::from_raw_parts(z as *mut u8, i_amt as usize);
        if let Err(err) = state.file.write_all(data) {
            return state.set_last_error(ffi::SQLITE_IOERR_WRITE, err);
        }

        ffi::SQLITE_OK
    }

    /// Truncate a file.
    pub unsafe extern "C" fn truncate<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        size: ffi::sqlite3_int64,
    ) -> c_int {
        log::trace!("truncate");

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSYNC,
        };
        log::trace!("truncate ({:?})", state.path);

        if let Err(err) = state.file.truncate(size as u64) {
            return state.set_last_error(ffi::SQLITE_IOERR_TRUNCATE, err);
        }

        ffi::SQLITE_OK
    }

    /// Persist changes to a file.
    pub unsafe extern "C" fn sync<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        flags: c_int,
    ) -> c_int {
        log::trace!("sync");

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSYNC,
        };
        log::trace!("sync ({:?})", state.path);

        #[cfg(feature = "sqlite_test")]
        {
            let is_full_sync = flags & 0x0F == ffi::SQLITE_SYNC_FULL;
            if is_full_sync {
                ffi::sqlite3_inc_fullsync_count();
            }
            ffi::sqlite3_inc_sync_count();
        }

        if let Err(err) = state.file.flush() {
            return state.set_last_error(ffi::SQLITE_IOERR_FSYNC, err);
        }

        if let Err(err) = state.file.sync(flags & ffi::SQLITE_SYNC_DATAONLY > 0) {
            return state.set_last_error(ffi::SQLITE_IOERR_FSYNC, err);
        }

        ffi::SQLITE_OK
    }

    /// Return the current file-size of a file.
    pub unsafe extern "C" fn file_size<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        p_size: *mut ffi::sqlite3_int64,
    ) -> c_int {
        log::trace!("file_size");

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSTAT,
        };
        log::trace!("file_size ({:?})", state.path);

        if let Err(err) = state.file.file_size().and_then(|n| {
            let p_size: &mut ffi::sqlite3_int64 = p_size.as_mut().ok_or_else(null_ptr_error)?;
            *p_size = n as ffi::sqlite3_int64;
            Ok(())
        }) {
            return state.set_last_error(ffi::SQLITE_IOERR_FSTAT, err);
        }

        ffi::SQLITE_OK
    }

    /// Lock a file.
    pub unsafe extern "C" fn lock<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        e_lock: c_int,
    ) -> c_int {
        log::trace!("lock");

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_LOCK,
        };
        log::trace!("lock ({:?})", state.path);

        let lock = match Lock::from_i32(e_lock) {
            Some(lock) => lock,
            None => return ffi::SQLITE_IOERR_LOCK,
        };
        match state.file.lock(lock) {
            Ok(true) => ffi::SQLITE_OK,
            Ok(false) => ffi::SQLITE_BUSY,
            Err(err) => state.set_last_error(ffi::SQLITE_IOERR_LOCK, err),
        }
    }

    /// Unlock a file.
    pub unsafe extern "C" fn unlock<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        e_lock: c_int,
    ) -> c_int {
        log::trace!("unlock");

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_UNLOCK,
        };
        log::trace!("unlock ({:?})", state.path);

        let lock = match Lock::from_i32(e_lock) {
            Some(lock) => lock,
            None => return ffi::SQLITE_IOERR_UNLOCK,
        };
        match state.file.unlock(lock) {
            Ok(true) => ffi::SQLITE_OK,
            Ok(false) => ffi::SQLITE_BUSY,
            Err(err) => state.set_last_error(ffi::SQLITE_IOERR_UNLOCK, err),
        }
    }

    /// Check if another file-handle holds a [Lock::Reserved] lock on a file.
    pub unsafe extern "C" fn check_reserved_lock<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        p_res_out: *mut c_int,
    ) -> c_int {
        log::trace!("check_reserved_lock");

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK,
        };
        log::trace!("unlock ({:?})", state.path);

        if let Err(err) = state.file.is_reserved().and_then(|is_reserved| {
            let p_res_out: &mut c_int = p_res_out.as_mut().ok_or_else(null_ptr_error)?;
            *p_res_out = is_reserved as c_int;
            Ok(())
        }) {
            return state.set_last_error(ffi::SQLITE_IOERR_UNLOCK, err);
        }

        ffi::SQLITE_OK
    }

    /// File control method. For custom operations on a mem-file.
    pub unsafe extern "C" fn file_control<F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        op: c_int,
        p_arg: *mut c_void,
    ) -> c_int {
        log::trace!("file_control op={}", op);

        let state = match file_state::<F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_NOTFOUND,
        };

        // eprintln!("file_control: {}", op);

        // Docs: https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html
        match op {
            // The following op codes are alreay handled by sqlite before, so no need to handle them
            // in a custom VFS.
            ffi::SQLITE_FCNTL_FILE_POINTER
            | ffi::SQLITE_FCNTL_VFS_POINTER
            | ffi::SQLITE_FCNTL_JOURNAL_POINTER
            | ffi::SQLITE_FCNTL_DATA_VERSION
            | ffi::SQLITE_FCNTL_RESERVE_BYTES => ffi::SQLITE_NOTFOUND,

            // The following op codes are no longer used and thus ignored.
            ffi::SQLITE_FCNTL_SYNC_OMITTED => ffi::SQLITE_NOTFOUND,

            // Used for debugging. Write current state of the lock into (int)pArg.
            ffi::SQLITE_FCNTL_LOCKSTATE => match state.file.current_lock() {
                Ok(lock) => {
                    if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                        *p_arg = lock as i32;
                    }
                    ffi::SQLITE_OK
                }
                Err(err) => state.set_last_error(ffi::SQLITE_ERROR, err),
            },

            // Relevant for proxy-type locking. Not implemented.
            ffi::SQLITE_FCNTL_GET_LOCKPROXYFILE | ffi::SQLITE_FCNTL_SET_LOCKPROXYFILE => {
                ffi::SQLITE_NOTFOUND
            }

            // Write last error number into (int)pArg.
            ffi::SQLITE_FCNTL_LAST_ERRNO => {
                if let Some((no, _)) = state.last_error.lock().unwrap().as_ref() {
                    if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                        *p_arg = *no;
                    }
                }
                ffi::SQLITE_OK
            }

            // Give the VFS layer a hint of how large the database file will grow to be during the
            // current transaction. Not implemented
            ffi::SQLITE_FCNTL_SIZE_HINT => ffi::SQLITE_OK,

            // Request that the VFS extends and truncates the database file in chunks of a size
            // specified by the user. Return an error as this is not forwarded to the [Vfs]  trait
            // right now.
            ffi::SQLITE_FCNTL_CHUNK_SIZE => {
                let chunk_size = match (p_arg as *mut i32)
                    .as_ref()
                    .cloned()
                    .and_then(|s| usize::try_from(s).ok())
                {
                    Some(chunk_size) => chunk_size,
                    None => {
                        return state.set_last_error(
                            ffi::SQLITE_NOTFOUND,
                            std::io::Error::new(std::io::ErrorKind::Other, "expect chunk_size arg"),
                        );
                    }
                };

                if let Err(err) = state.file.set_chunk_size(chunk_size) {
                    return state.set_last_error(ffi::SQLITE_ERROR, err);
                }

                ffi::SQLITE_OK
            }

            // Configure automatic retry counts and intervals for certain disk I/O operations for
            // the windows VFS in order to provide robustness in the presence of anti-virus
            // programs. Not implemented.
            ffi::SQLITE_FCNTL_WIN32_AV_RETRY => ffi::SQLITE_NOTFOUND,

            // Enable or disable the persistent WAL setting. Not implemented,
            ffi::SQLITE_FCNTL_PERSIST_WAL => ffi::SQLITE_NOTFOUND,

            // Indicate that, unless it is rolled back for some reason, the entire database file
            // will be overwritten by the current transaction. Not implemented.
            ffi::SQLITE_FCNTL_OVERWRITE => ffi::SQLITE_NOTFOUND,

            // Used to obtain the names of all VFSes in the VFS stack.
            ffi::SQLITE_FCNTL_VFSNAME => {
                if let Some(p_arg) = (p_arg as *mut *const c_char).as_mut() {
                    let name = ManuallyDrop::new(state.vfs_name.clone());
                    *p_arg = name.as_ptr();
                };

                ffi::SQLITE_OK
            }

            // Set or query the persistent "powersafe-overwrite" or "PSOW" setting. Not implemented.
            ffi::SQLITE_FCNTL_POWERSAFE_OVERWRITE => ffi::SQLITE_NOTFOUND,

            // Optionally intercept PRAGMA statements. Always fall back to normal pragma processing.
            ffi::SQLITE_FCNTL_PRAGMA => ffi::SQLITE_NOTFOUND,

            // May be invoked by SQLite on the database file handle shortly after it is opened in
            // order to provide a custom VFS with access to the connection's busy-handler callback.
            // Not implemetned.
            ffi::SQLITE_FCNTL_BUSYHANDLER => ffi::SQLITE_NOTFOUND,

            // Generate a temporary filename. Not implemented.
            ffi::SQLITE_FCNTL_TEMPFILENAME => ffi::SQLITE_NOTFOUND,

            // Query or set the maximum number of bytes that will be used for memory-mapped I/O.
            // Not implemented.
            ffi::SQLITE_FCNTL_MMAP_SIZE => ffi::SQLITE_NOTFOUND,

            // Advisory information to the VFS about what the higher layers of the SQLite stack are
            // doing.
            ffi::SQLITE_FCNTL_TRACE => {
                let trace = CStr::from_ptr(p_arg as *const c_char);
                log::trace!("{}", trace.to_string_lossy());
                ffi::SQLITE_OK
            }

            // Check whether or not the file has been renamed, moved, or deleted since it was first
            // opened. Not implemented.
            ffi::SQLITE_FCNTL_HAS_MOVED => ffi::SQLITE_NOTFOUND,

            // Sent to the VFS immediately before the xSync method is invoked on a database file
            // descriptor. Silently ignored.
            ffi::SQLITE_FCNTL_SYNC => ffi::SQLITE_OK,

            // Sent to the VFS after a transaction has been committed immediately but before the
            // database is unlocked. Silently ignored.
            ffi::SQLITE_FCNTL_COMMIT_PHASETWO => ffi::SQLITE_OK,

            // Used for debugging. Sswap the file handle with the one pointed to by the pArg
            // argument. This capability is used during testing and only needs to be supported when
            // SQLITE_TEST is defined. Not implemented.
            ffi::SQLITE_FCNTL_WIN32_SET_HANDLE => ffi::SQLITE_NOTFOUND,

            // Signal to the VFS layer that it might be advantageous to block on the next WAL lock
            // if the lock is not immediately available. The WAL subsystem issues this signal during
            // rare circumstances in order to fix a problem with priority inversion.
            // Not implemented.
            ffi::SQLITE_FCNTL_WAL_BLOCK => ffi::SQLITE_NOTFOUND,

            // Implemented by zipvfs only.
            ffi::SQLITE_FCNTL_ZIPVFS => ffi::SQLITE_NOTFOUND,

            // Implemented by the special VFS used by the RBU extension only.
            ffi::SQLITE_FCNTL_RBU => ffi::SQLITE_NOTFOUND,

            // Obtain the underlying native file handle associated with a file handle.
            // Not implemented.
            ffi::SQLITE_FCNTL_WIN32_GET_HANDLE => ffi::SQLITE_NOTFOUND,

            // Usage is not documented. Not implemented.
            ffi::SQLITE_FCNTL_PDB => ffi::SQLITE_NOTFOUND,

            // Used for "batch write mode". Not supported.
            ffi::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE
            | ffi::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE
            | ffi::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => ffi::SQLITE_NOTFOUND,

            // Configure a VFS to block for up to M milliseconds before failing when attempting to
            // obtain a file lock using the xLock or xShmLock methods of the VFS. Not implemented.
            ffi::SQLITE_FCNTL_LOCK_TIMEOUT => ffi::SQLITE_NOTFOUND,

            // Used by in-mremory VFS.
            ffi::SQLITE_FCNTL_SIZE_LIMIT => ffi::SQLITE_NOTFOUND,

            // Invoked from within a checkpoint in wal mode after the client has finished copying
            // pages from the wal file to the database file, but before the *-shm file is updated to
            // record the fact that the pages have been checkpointed. Not implemented.
            ffi::SQLITE_FCNTL_CKPT_DONE => ffi::SQLITE_NOTFOUND,

            // Invoked from within a checkpoint in wal mode before the client starts to copy pages
            // from the wal file to the database file. Not implemented.
            ffi::SQLITE_FCNTL_CKPT_START => ffi::SQLITE_NOTFOUND,

            _ => ffi::SQLITE_NOTFOUND,
            // The following op codes are mentioned in the docs but are not defined in ffi::*:
            // SQLITE_FCNTL_EXTERNAL_READER, SQLITE_FCNTL_CKSM_FILE
        }
    }

    /// Return the sector-size in bytes for a file.
    pub unsafe extern "C" fn sector_size<F>(_p_file: *mut ffi::sqlite3_file) -> c_int {
        log::trace!("sector_size");

        1024
    }

    /// Return the device characteristic flags supported by a file.
    pub unsafe extern "C" fn device_characteristics<F>(_p_file: *mut ffi::sqlite3_file) -> c_int {
        log::trace!("device_characteristics");

        // For now, simply copied from [memfs] without putting in a lot of thought.
        // [memfs]: (https://github.com/sqlite/sqlite/blob/a959bf53110bfada67a3a52187acd57aa2f34e19/ext/misc/memvfs.c#L271-L276)

        // writes of any size are atomic
        ffi::SQLITE_IOCAP_ATOMIC |
        // after reboot following a crash or power loss, the only bytes in a file that were written
        // at the application level might have changed and that adjacent bytes, even bytes within
        // the same sector are guaranteed to be unchanged
        ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE |
        // when data is appended to a file, the data is appended first then the size of the file is
        // extended, never the other way around
        ffi::SQLITE_IOCAP_SAFE_APPEND |
        // information is written to disk in the same order as calls to xWrite()
        ffi::SQLITE_IOCAP_SEQUENTIAL
    }

    /// Create a shared memory file mapping.
    pub unsafe extern "C" fn shm_map<F>(
        _p_file: *mut ffi::sqlite3_file,
        i_pg: i32,
        pgsz: i32,
        b_extend: i32,
        _pp: *mut *mut c_void,
    ) -> i32 {
        log::trace!("shm_map pg={} sz={} extend={}", i_pg, pgsz, b_extend);

        ffi::SQLITE_IOERR_SHMMAP
    }

    /// Perform locking on a shared-memory segment.
    pub unsafe extern "C" fn shm_lock<F>(
        _p_file: *mut ffi::sqlite3_file,
        _offset: i32,
        _n: i32,
        _flags: i32,
    ) -> i32 {
        log::trace!("shm_lock");

        ffi::SQLITE_IOERR_SHMLOCK
    }

    /// Memory barrier operation on shared memory.
    pub unsafe extern "C" fn shm_barrier(_p_file: *mut ffi::sqlite3_file) {
        log::trace!("shm_barrier");
    }

    /// Unmap a shared memory segment.
    pub unsafe extern "C" fn shm_unmap<F>(
        _p_file: *mut ffi::sqlite3_file,
        _delete_flags: i32,
    ) -> i32 {
        log::trace!("shm_unmap");

        ffi::SQLITE_OK
    }

    /// Fetch a page of a memory-mapped file.
    pub unsafe extern "C" fn mem_fetch<F: DatabaseHandle>(
        _p_file: *mut ffi::sqlite3_file,
        i_ofst: i64,
        i_amt: i32,
        _pp: *mut *mut c_void,
    ) -> i32 {
        log::trace!("mem_fetch offset={} len={}", i_ofst, i_amt);

        ffi::SQLITE_ERROR
    }

    /// Release a memory-mapped page.
    pub unsafe extern "C" fn mem_unfetch<F>(
        _p_file: *mut ffi::sqlite3_file,
        i_ofst: i64,
        _p_page: *mut c_void,
    ) -> i32 {
        log::trace!("mem_unfetch offset={}", i_ofst);

        ffi::SQLITE_OK
    }
}

impl<V> State<V> {
    fn set_last_error(&mut self, no: i32, err: std::io::Error) -> i32 {
        *(self.last_error.lock().unwrap()) = Some((no, err));
        no
    }
}

impl<F> FileExt<F> {
    fn set_last_error(&mut self, no: i32, err: std::io::Error) -> i32 {
        *(self.last_error.lock().unwrap()) = Some((no, err));
        no
    }
}

fn null_ptr_error() -> std::io::Error {
    std::io::Error::new(ErrorKind::Other, "received null pointer")
}

unsafe fn vfs_state<'a, V>(ptr: *mut ffi::sqlite3_vfs) -> Result<&'a mut State<V>, std::io::Error> {
    let vfs: &mut ffi::sqlite3_vfs = ptr.as_mut().ok_or_else(null_ptr_error)?;
    let state = (vfs.pAppData as *mut State<V>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    Ok(state)
}

unsafe fn file_state<'a, F>(
    ptr: *mut ffi::sqlite3_file,
) -> Result<&'a mut FileExt<F>, std::io::Error> {
    let f = (ptr as *mut FileState<F>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    let ext = f.ext.assume_init_mut();
    Ok(ext)
}

impl OpenOptions {
    fn from_flags(flags: i32) -> Option<Self> {
        Some(OpenOptions {
            kind: OpenKind::from_flags(flags)?,
            access: OpenAccess::from_flags(flags)?,
            delete_on_close: flags & ffi::SQLITE_OPEN_DELETEONCLOSE > 0,
        })
    }

    fn to_flags(&self) -> i32 {
        self.kind.to_flags()
            | self.access.to_flags()
            | if self.delete_on_close {
                ffi::SQLITE_OPEN_DELETEONCLOSE
            } else {
                0
            }
    }
}

impl OpenKind {
    fn from_flags(flags: i32) -> Option<Self> {
        match flags {
            flags if flags & ffi::SQLITE_OPEN_MAIN_DB > 0 => Some(Self::MainDb),
            flags if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL > 0 => Some(Self::MainJournal),
            flags if flags & ffi::SQLITE_OPEN_TEMP_DB > 0 => Some(Self::TempDb),
            flags if flags & ffi::SQLITE_OPEN_TEMP_JOURNAL > 0 => Some(Self::TempJournal),
            flags if flags & ffi::SQLITE_OPEN_TRANSIENT_DB > 0 => Some(Self::TransientDb),
            flags if flags & ffi::SQLITE_OPEN_SUBJOURNAL > 0 => Some(Self::SubJournal),
            flags if flags & ffi::SQLITE_OPEN_SUPER_JOURNAL > 0 => Some(Self::SuperJournal),
            flags if flags & ffi::SQLITE_OPEN_WAL > 0 => Some(Self::Wal),
            _ => None,
        }
    }

    fn to_flags(self) -> i32 {
        match self {
            OpenKind::MainDb => ffi::SQLITE_OPEN_MAIN_DB,
            OpenKind::MainJournal => ffi::SQLITE_OPEN_MAIN_JOURNAL,
            OpenKind::TempDb => ffi::SQLITE_OPEN_TEMP_DB,
            OpenKind::TempJournal => ffi::SQLITE_OPEN_TEMP_JOURNAL,
            OpenKind::TransientDb => ffi::SQLITE_OPEN_TRANSIENT_DB,
            OpenKind::SubJournal => ffi::SQLITE_OPEN_SUBJOURNAL,
            OpenKind::SuperJournal => ffi::SQLITE_OPEN_SUPER_JOURNAL,
            OpenKind::Wal => ffi::SQLITE_OPEN_WAL,
        }
    }
}

impl OpenAccess {
    fn from_flags(flags: i32) -> Option<Self> {
        match flags {
            flags
                if (flags & ffi::SQLITE_OPEN_CREATE > 0)
                    && (flags & ffi::SQLITE_OPEN_EXCLUSIVE > 0) =>
            {
                Some(Self::CreateNew)
            }
            flags if flags & ffi::SQLITE_OPEN_CREATE > 0 => Some(Self::Create),
            flags if flags & ffi::SQLITE_OPEN_READWRITE > 0 => Some(Self::Write),
            flags if flags & ffi::SQLITE_OPEN_READONLY > 0 => Some(Self::Read),
            _ => None,
        }
    }

    fn to_flags(self) -> i32 {
        match self {
            OpenAccess::Read => ffi::SQLITE_OPEN_READONLY,
            OpenAccess::Write => ffi::SQLITE_OPEN_READWRITE,
            OpenAccess::Create => ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
            OpenAccess::CreateNew => {
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_EXCLUSIVE
            }
        }
    }
}

impl Lock {
    fn from_i32(lock: i32) -> Option<Self> {
        Some(match lock {
            ffi::SQLITE_LOCK_NONE => Self::None,
            ffi::SQLITE_LOCK_SHARED => Self::Shared,
            ffi::SQLITE_LOCK_RESERVED => Self::Reserved,
            ffi::SQLITE_LOCK_PENDING => Self::Pending,
            ffi::SQLITE_LOCK_EXCLUSIVE => Self::Exclusive,
            _ => return None,
        })
    }

    fn to_i32(self) -> i32 {
        match self {
            Self::None => ffi::SQLITE_LOCK_NONE,
            Self::Shared => ffi::SQLITE_LOCK_SHARED,
            Self::Reserved => ffi::SQLITE_LOCK_RESERVED,
            Self::Pending => ffi::SQLITE_LOCK_PENDING,
            Self::Exclusive => ffi::SQLITE_LOCK_EXCLUSIVE,
        }
    }
}

impl PartialOrd for Lock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.to_i32().partial_cmp(&other.to_i32())
    }
}

impl Default for Lock {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug)]
pub enum RegisterError {
    Nul(std::ffi::NulError),
    Register(i32),
}

impl std::error::Error for RegisterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Nul(err) => Some(err),
            Self::Register(_) => None,
        }
    }
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nul(_) => f.write_str("interior nul byte in name found"),
            Self::Register(code) => {
                write!(f, "registering sqlite vfs failed with error code: {}", code)
            }
        }
    }
}

impl From<std::ffi::NulError> for RegisterError {
    fn from(err: std::ffi::NulError) -> Self {
        Self::Nul(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_order() {
        assert!(Lock::None < Lock::Shared);
        assert!(Lock::Shared < Lock::Reserved);
        assert!(Lock::Reserved < Lock::Pending);
        assert!(Lock::Pending < Lock::Exclusive);
    }
}
