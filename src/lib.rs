use core::slice;
use std::cell::Cell;
use std::ffi::{c_void, CStr, CString};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, ManuallyDrop};
use std::os::raw::{c_char, c_int};
use std::path::Path;
use std::ptr::null;
use std::ptr::null_mut;
use std::rc::Rc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use rusqlite::ffi;
pub use rusqlite::OpenFlags;

pub trait File: Read + Seek + Write {
    fn file_size(&self) -> Result<u64, std::io::Error>;
}

pub trait Vfs {
    type File: File;

    fn open(&self, path: &Path, flags: OpenFlags) -> Result<Self::File, std::io::Error>;
    fn delete(&self, path: &Path) -> Result<(), std::io::Error>;
    fn exists(&self, _path: &Path) -> Result<bool, std::io::Error>;

    fn access(&self, _path: &Path, _write: bool) -> Result<bool, std::io::Error> {
        Ok(true)
    }
}

struct State<V> {
    vfs: V,
    io_methods: ffi::sqlite3_io_methods,
    last_error: Rc<Cell<Option<std::io::Error>>>,
}

pub fn register<F: File, V: Vfs<File = F>>(name: &str, vfs: V) -> Result<(), std::ffi::NulError> {
    let name = ManuallyDrop::new(CString::new(name)?);
    let io_methods = ffi::sqlite3_io_methods {
        iVersion: 3,
        xClose: Some(io::close::<F>),
        xRead: Some(io::read::<F>),
        xWrite: Some(io::write::<F>),
        xTruncate: Some(io::truncate),
        xSync: Some(io::sync::<F>),
        xFileSize: Some(io::file_size::<F>),
        xLock: Some(io::lock),
        xUnlock: Some(io::unlock),
        xCheckReservedLock: Some(io::check_reserved_lock),
        xFileControl: Some(io::file_control),
        xSectorSize: Some(io::sector_size),
        xDeviceCharacteristics: Some(io::device_characteristics),
        xShmMap: Some(io::shm_map),
        xShmLock: Some(io::shm_lock),
        xShmBarrier: Some(io::shm_barrier),
        xShmUnmap: Some(io::shm_unmap),
        xFetch: Some(io::mem_fetch::<F>),
        xUnfetch: Some(io::mem_unfetch),
    };
    let ptr = Box::into_raw(Box::new(State {
        vfs,
        io_methods,
        last_error: Default::default(),
    }));
    let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        iVersion: 3,
        szOsFile: size_of::<FileState<F>>() as i32,
        mxPathname: MAX_PATH_LENGTH as i32, // max path length supported by VFS
        pNext: null_mut(),
        zName: name.as_ptr(),
        pAppData: ptr as _,
        xOpen: Some(vfs::open::<F, V>),
        xDelete: Some(vfs::delete::<V>),
        xAccess: Some(vfs::access::<V>),
        xFullPathname: Some(vfs::full_pathname),
        xDlOpen: Some(vfs::dlopen),
        xDlError: Some(vfs::dlerror),
        xDlSym: Some(vfs::dlsym),
        xDlClose: Some(vfs::dlclose),
        xRandomness: Some(vfs::randomness),
        xSleep: Some(vfs::sleep),
        xCurrentTime: Some(vfs::current_time),
        xGetLastError: Some(vfs::get_last_error),
        xCurrentTimeInt64: Some(vfs::current_time_int64),
        xSetSystemCall: None,
        xGetSystemCall: None,
        xNextSystemCall: None,
    }));

    let result = unsafe { ffi::sqlite3_vfs_register(vfs, false as i32) };
    if result != ffi::SQLITE_OK {
        // TODO: proper error
        panic!("not ok! {}", result);
    }

    // TODO: return object that allows to unregister (and cleanup the memory)

    Ok(())
}

const MAX_PATH_LENGTH: usize = 512;

#[repr(C)]
struct FileState<F> {
    base: ffi::sqlite3_file,
    name: *mut i8,
    file: *mut F,
    last_error: *const Cell<Option<std::io::Error>>,
}

// Example mem-fs implementation:
// https://github.com/sqlite/sqlite/blob/a959bf53110bfada67a3a52187acd57aa2f34e19/ext/misc/memvfs.c
mod vfs {

    use super::*;

    /// Open a new file handler.
    pub unsafe extern "C" fn open<F: File, V: Vfs<File = F>>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        p_file: *mut ffi::sqlite3_file,
        flags: c_int,
        _p_out_flags: *mut c_int,
    ) -> c_int {
        let name = if z_name.is_null() {
            None
        } else {
            CStr::from_ptr(z_name).to_str().ok()
        };
        println!("open z_name={:?} flags={}", name, flags);

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        state.last_error.take();

        let path = CStr::from_ptr(z_name);
        // TODO: any way to use OsStr instead?
        let path = path.to_string_lossy().to_string();

        if let Err(err) = state
            .vfs
            .open(path.as_ref(), OpenFlags::from_bits_unchecked(flags))
            .and_then(|f| {
                let out_file = (p_file as *mut FileState<F>)
                    .as_mut()
                    .ok_or_else(null_ptr_error)?;
                out_file.base.pMethods = &state.io_methods;
                // TODO: unwrap
                out_file.name = CString::new(name.unwrap().to_string()).unwrap().into_raw();
                out_file.file = Box::into_raw(Box::new(f));
                out_file.last_error = Rc::into_raw(Rc::clone(&state.last_error));
                Ok(())
            })
        {
            eprintln!("OPEN ERR: {}", err);
            state.last_error.set(Some(err));
            return ffi::SQLITE_CANTOPEN;
        }

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
        println!("delete z_name={:?}", name);

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_DELETE,
        };
        state.last_error.take();

        let path = CStr::from_ptr(z_path);
        // TODO: any way to use OsStr instead?
        let path = path.to_string_lossy().to_string();

        match state.vfs.delete(path.as_ref()) {
            Ok(_) => ffi::SQLITE_OK,
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    ffi::SQLITE_OK
                } else {
                    state.last_error.set(Some(err));
                    ffi::SQLITE_DELETE
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
        println!("access z_name={:?} flags={}", name, flags);

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        state.last_error.take();

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
            state.last_error.set(Some(err));
            return ffi::SQLITE_IOERR_ACCESS;
        }

        ffi::SQLITE_OK
    }

    /// Populate buffer `z_out` with the full canonical pathname corresponding to the pathname in
    /// `z_path`. `z_out` is guaranteed to point to a buffer of at least (INST_MAX_PATHNAME+1)
    /// bytes.
    pub unsafe extern "C" fn full_pathname(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        n_out: c_int,
        z_out: *mut c_char,
    ) -> c_int {
        let name = CStr::from_ptr(z_path);
        println!("full_pathname name={}", name.to_string_lossy());

        let state = match vfs_state::<()>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        state.last_error.take();

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
        println!("dlopen");

        null_mut()
    }

    /// Populate the buffer `z_err_msg` (size `n_byte` bytes) with a human readable utf-8 string
    /// describing the most recent error encountered associated with dynamic libraries.
    pub unsafe extern "C" fn dlerror(
        _p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) {
        println!("dlerror");

        let msg = concat!("Loadable extensions are not supported", "\0");
        ffi::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr() as _);
    }

    /// Return a pointer to the symbol `z_sym` in the dynamic library pHandle.
    pub unsafe extern "C" fn dlsym(
        _p_vfs: *mut ffi::sqlite3_vfs,
        _p: *mut c_void,
        _z_sym: *const c_char,
    ) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const i8)> {
        println!("dlsym");

        None
    }

    /// Close the dynamic library handle `p_handle`.
    pub unsafe extern "C" fn dlclose(_p_vfs: *mut ffi::sqlite3_vfs, _p_handle: *mut c_void) {
        println!("dlclose");
    }

    /// Populate the buffer pointed to by `z_buf_out` with `n_byte` bytes of random data.
    pub unsafe extern "C" fn randomness(
        _p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_buf_out: *mut c_char,
    ) -> c_int {
        println!("randomness");

        use rand::Rng;

        let bytes = slice::from_raw_parts_mut(z_buf_out, n_byte as usize);
        rand::thread_rng().fill(bytes);
        bytes.len() as c_int
    }

    /// Sleep for `n_micro` microseconds. Return the number of microseconds actually slept.
    pub unsafe extern "C" fn sleep(_p_vfs: *mut ffi::sqlite3_vfs, n_micro: c_int) -> c_int {
        println!("sleep");

        let instant = Instant::now();
        thread::sleep(Duration::from_micros(n_micro as u64));
        instant.elapsed().as_micros() as c_int
    }

    /// Return the current time as a Julian Day number in `p_time_out`.
    pub unsafe extern "C" fn current_time(
        p_vfs: *mut ffi::sqlite3_vfs,
        p_time_out: *mut f64,
    ) -> c_int {
        println!("current_time");

        let state = match vfs_state::<()>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        state.last_error.take();

        let now = time::OffsetDateTime::now_utc().unix_timestamp() as f64;
        *p_time_out = 2440587.5 + now / 864.0e5;
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn get_last_error(
        p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) -> c_int {
        let state = match vfs_state::<()>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        if let Some(err) = state.last_error.take() {
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

    pub unsafe extern "C" fn current_time_int64(p_vfs: *mut ffi::sqlite3_vfs, p: *mut i64) -> i32 {
        println!("current_time_int64");

        let state = match vfs_state::<()>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        state.last_error.take();

        let now = time::OffsetDateTime::now_utc().unix_timestamp() as f64;
        *p = ((2440587.5 + now / 864.0e5) * 864.0e5) as i64;
        ffi::SQLITE_OK
    }
}

mod io {
    use super::*;

    /// Close a file.
    pub unsafe extern "C" fn close<F>(p_file: *mut ffi::sqlite3_file) -> c_int {
        println!("close");

        let state = match file_state::<F>(p_file, true) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CLOSE,
        };
        println!("    ({})", CStr::from_ptr(state.name).to_string_lossy());

        // TODO: only when free on close is set?
        drop(CString::from_raw(state.name));
        state.name = null_mut();

        Box::from_raw(state.file);
        state.file = null_mut();

        Rc::from_raw(state.last_error);
        state.last_error = null();

        ffi::SQLITE_OK
    }

    /// Read data from a file.
    pub unsafe extern "C" fn read<F: File>(
        p_file: *mut ffi::sqlite3_file,
        z_buf: *mut c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        println!("read offset={} len={}", i_ofst, i_amt);

        let state = match file_state::<F>(p_file, true) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CLOSE,
        };
        println!("    ({})", CStr::from_ptr(state.name).to_string_lossy());
        let file = match file::<F>(state.file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CLOSE,
        };

        match file.seek(SeekFrom::Start(i_ofst as u64)) {
            Ok(o) => {
                if o != i_ofst as u64 {
                    return ffi::SQLITE_IOERR_READ;
                }
            }
            Err(err) => {
                state.set_last_error(err);
                return ffi::SQLITE_IOERR_READ;
            }
        }

        let out = slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize);
        if let Err(err) = file.read_exact(out) {
            let kind = err.kind();
            if kind == ErrorKind::UnexpectedEof {
                return ffi::SQLITE_IOERR_SHORT_READ;
            } else {
                state.set_last_error(err);
                return ffi::SQLITE_IOERR_READ;
            }
        }

        ffi::SQLITE_OK
    }

    /// Write data to a file.
    pub unsafe extern "C" fn write<F: File>(
        p_file: *mut ffi::sqlite3_file,
        z: *const c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        println!("write offset={} len={}", i_ofst, i_amt);

        let state = match file_state::<F>(p_file, true) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_WRITE,
        };
        println!("    ({})", CStr::from_ptr(state.name).to_string_lossy());
        let file = match file::<F>(state.file) {
            Ok(f) => f,
            Err(err) => {
                state.set_last_error(err);
                return ffi::SQLITE_IOERR_WRITE;
            }
        };

        match file.seek(SeekFrom::Start(i_ofst as u64)) {
            Ok(o) => {
                if o != i_ofst as u64 {
                    return ffi::SQLITE_IOERR_WRITE;
                }
            }
            Err(err) => {
                state.set_last_error(err);
                return ffi::SQLITE_IOERR_WRITE;
            }
        }

        let data = slice::from_raw_parts(z as *mut u8, i_amt as usize);
        if let Err(err) = file.write_all(data) {
            state.set_last_error(err);
            return ffi::SQLITE_IOERR_WRITE;
        }

        ffi::SQLITE_OK
    }

    /// Truncate a file.
    pub unsafe extern "C" fn truncate(
        _p_file: *mut ffi::sqlite3_file,
        _size: ffi::sqlite3_int64,
    ) -> c_int {
        println!("truncate");
        todo!("truncate");
    }

    /// Persist changes to a file.
    pub unsafe extern "C" fn sync<F: File>(p_file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
        println!("sync");

        let state = match file_state::<F>(p_file, true) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSYNC,
        };
        println!("    ({})", CStr::from_ptr(state.name).to_string_lossy());
        let file = match file::<F>(state.file) {
            Ok(f) => f,
            Err(err) => {
                state.set_last_error(err);
                return ffi::SQLITE_IOERR_FSYNC;
            }
        };

        if let Err(err) = file.flush() {
            state.set_last_error(err);
            return ffi::SQLITE_IOERR_FSYNC;
        }

        ffi::SQLITE_OK
    }

    /// Return the current file-size of a file.
    pub unsafe extern "C" fn file_size<F: File>(
        p_file: *mut ffi::sqlite3_file,
        p_size: *mut ffi::sqlite3_int64,
    ) -> c_int {
        println!("file_size");

        let state = match file_state::<F>(p_file, true) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSTAT,
        };
        println!("    ({})", CStr::from_ptr(state.name).to_string_lossy());
        let file = match file::<F>(state.file) {
            Ok(f) => f,
            Err(err) => {
                state.set_last_error(err);
                return ffi::SQLITE_IOERR_FSTAT;
            }
        };

        if let Err(err) = file.file_size().and_then(|n| {
            let p_size: &mut ffi::sqlite3_int64 = p_size.as_mut().ok_or_else(null_ptr_error)?;
            *p_size = n as ffi::sqlite3_int64;
            Ok(())
        }) {
            state.set_last_error(err);
            return ffi::SQLITE_IOERR_FSTAT;
        }

        ffi::SQLITE_OK
    }

    /// Lock a file.
    pub unsafe extern "C" fn lock(p_file: *mut ffi::sqlite3_file, _e_lock: c_int) -> c_int {
        println!("lock");

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_IOERR_LOCK;
        }

        // TODO: implement locking
        ffi::SQLITE_OK
    }

    /// Unlock a file.
    pub unsafe extern "C" fn unlock(p_file: *mut ffi::sqlite3_file, _e_lock: c_int) -> c_int {
        println!("unlock");

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_IOERR_UNLOCK;
        }

        // TODO: implement locking
        ffi::SQLITE_OK
    }

    /// Check if another file-handle holds a RESERVED lock on a file.
    pub unsafe extern "C" fn check_reserved_lock(
        p_file: *mut ffi::sqlite3_file,
        p_res_out: *mut c_int,
    ) -> c_int {
        println!("check_reserved_lock");

        let state = match file_state::<()>(p_file, true) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK,
        };

        match p_res_out.as_mut() {
            Some(p_res_out) => {
                *p_res_out = false as i32;
            }
            None => {
                state.set_last_error(null_ptr_error());
                return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK;
            }
        }

        // TODO: implement locking
        ffi::SQLITE_OK
    }

    /// File control method. For custom operations on an mem-file.
    pub unsafe extern "C" fn file_control(
        p_file: *mut ffi::sqlite3_file,
        op: c_int,
        _p_arg: *mut c_void,
    ) -> c_int {
        println!("file_control op={}", op);

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_ERROR;
        }

        ffi::SQLITE_NOTFOUND
    }

    /// Return the sector-size in bytes for a file.
    pub unsafe extern "C" fn sector_size(p_file: *mut ffi::sqlite3_file) -> c_int {
        println!("sector_size");

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_ERROR;
        }

        1024
    }

    /// Return the device characteristic flags supported by a file.
    pub unsafe extern "C" fn device_characteristics(p_file: *mut ffi::sqlite3_file) -> c_int {
        println!("device_characteristics");

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_ERROR;
        }

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
    pub unsafe extern "C" fn shm_map(
        p_file: *mut ffi::sqlite3_file,
        i_pg: i32,
        pgsz: i32,
        b_extend: i32,
        _pp: *mut *mut c_void,
    ) -> i32 {
        println!("shm_map pg={} sz={} extend={}", i_pg, pgsz, b_extend);

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_IOERR_SHMMAP;
        }

        ffi::SQLITE_IOERR_SHMMAP
    }

    /// Perform locking on a shared-memory segment.
    pub unsafe extern "C" fn shm_lock(
        p_file: *mut ffi::sqlite3_file,
        _offset: i32,
        _n: i32,
        _flags: i32,
    ) -> i32 {
        println!("shm_lock");

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_IOERR_SHMMAP;
        }

        ffi::SQLITE_IOERR_SHMLOCK
    }

    /// Memory barrier operation on shared memory.
    pub unsafe extern "C" fn shm_barrier(_p_file: *mut ffi::sqlite3_file) {
        println!("shm_barrier");
    }

    /// Unmap a shared memory segment.
    pub unsafe extern "C" fn shm_unmap(p_file: *mut ffi::sqlite3_file, _delete_flags: i32) -> i32 {
        println!("shm_unmap");

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_IOERR_SHMMAP;
        }

        ffi::SQLITE_OK
    }

    /// Fetch a page of a memory-mapped file.
    pub unsafe extern "C" fn mem_fetch<F: File>(
        p_file: *mut ffi::sqlite3_file,
        i_ofst: i64,
        i_amt: i32,
        _pp: *mut *mut c_void,
    ) -> i32 {
        println!("mem_fetch offset={} len={}", i_ofst, i_amt);

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_ERROR;
        }

        ffi::SQLITE_ERROR
    }

    /// Release a memory-mapped page.
    pub unsafe extern "C" fn mem_unfetch(
        p_file: *mut ffi::sqlite3_file,
        i_ofst: i64,
        _p_page: *mut c_void,
    ) -> i32 {
        println!("mem_unfetch offset={}", i_ofst);

        // reset last error
        if file_state::<()>(p_file, true).is_err() {
            return ffi::SQLITE_ERROR;
        }

        ffi::SQLITE_OK
    }
}

impl<F> FileState<F> {
    unsafe fn unset_last_error(&mut self) {
        let last_error = Rc::from_raw(self.last_error);
        last_error.take();
        self.last_error = Rc::into_raw(last_error);
    }

    unsafe fn set_last_error(&mut self, err: std::io::Error) {
        let last_error = Rc::from_raw(self.last_error);
        last_error.set(Some(err));
        self.last_error = Rc::into_raw(last_error);
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
    reset_last_error: bool,
) -> Result<&'a mut FileState<F>, std::io::Error> {
    let f = (ptr as *mut FileState<F>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    if reset_last_error {
        f.unset_last_error();
    }
    Ok(f)
}

unsafe fn file<'a, F>(ptr: *mut F) -> Result<&'a mut F, std::io::Error> {
    let f: &mut F = ptr.as_mut().ok_or_else(null_ptr_error)?;
    Ok(f)
}

impl<F> Drop for FileState<F> {
    fn drop(&mut self) {
        unsafe {
            drop(CString::from_raw(self.name));
            Box::from_raw(self.file);
            Rc::from_raw(self.last_error);
        };
    }
}

impl File for std::fs::File {
    fn file_size(&self) -> Result<u64, std::io::Error> {
        Ok(dbg!(self.metadata()?.len()))
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
