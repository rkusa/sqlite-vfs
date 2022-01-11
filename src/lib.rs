use core::slice;
use std::ffi::{c_void, CStr, CString, OsStr};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, ManuallyDrop};
use std::os::raw::{c_char, c_int};
use std::os::unix::prelude::OsStrExt;
use std::path::Path;
use std::ptr::{null_mut, NonNull};
use std::thread;
use std::time::Duration;

use rusqlite::ffi;

pub trait File: Read + Seek + Write {
    fn file_size(&self) -> Result<u64, std::io::Error>;
}

pub trait Vfs {
    type File: File;

    fn open(&self, path: &Path) -> Result<Self::File, std::io::Error>;
    fn delete(&self, path: &Path) -> Result<(), std::io::Error>;

    fn access(&self, _path: &Path) -> Result<bool, std::io::Error> {
        Ok(true)
    }
}

struct State<V> {
    vfs: V,
    io_methods: ffi::sqlite3_io_methods,
}

pub fn register<F: File, V: Vfs<File = F>>(name: &str, vfs: V) {
    let name = ManuallyDrop::new(CString::new(name).unwrap());
    let io_methods = ffi::sqlite3_io_methods {
        iVersion: 1,
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
        // TODO: implement v2?
        xShmMap: None,
        xShmLock: None,
        xShmBarrier: None,
        xShmUnmap: None,
        // TODO: implement v3?
        xFetch: None,
        xUnfetch: None,
    };
    let ptr = Box::into_raw(Box::new(State { vfs, io_methods }));
    let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        iVersion: 1,
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
        xCurrentTimeInt64: None,
        xSetSystemCall: None,
        xGetSystemCall: None,
        xNextSystemCall: None,
    }));

    if unsafe { ffi::sqlite3_vfs_register(vfs, false as i32) } != ffi::SQLITE_OK {
        panic!("not ok!");
    }

    // TODO: return object that allows to unregister (and cleanup the memory)
}

const MAX_PATH_LENGTH: usize = 512;

#[repr(C)]
struct FileState<F> {
    base: ffi::sqlite3_file,
    file: *mut F,
}

// Example mem-fs implementation:
// https://github.com/sqlite/sqlite/blob/a959bf53110bfada67a3a52187acd57aa2f34e19/ext/misc/memvfs.c
mod vfs {
    use std::io::ErrorKind;
    use std::time::Instant;

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

        let vfs: &mut ffi::sqlite3_vfs = p_vfs.as_mut().expect("vfs is null");
        let mut state: std::ptr::NonNull<State<V>> =
            NonNull::new(vfs.pAppData as _).expect("pAppData is null");
        let state = state.as_mut();

        let slice = CStr::from_ptr(z_name);
        let osstr = OsStr::from_bytes(slice.to_bytes());

        match state.vfs.open(osstr.as_ref()) {
            Ok(f) => {
                // TODO: p_out_flags?
                let mut out_file = NonNull::new(p_file as *mut FileState<F>).unwrap();
                let out_file = out_file.as_mut();
                out_file.base.pMethods = &state.io_methods;
                out_file.file = Box::into_raw(Box::new(f));

                ffi::SQLITE_OK
            }
            Err(_) => ffi::SQLITE_CANTOPEN,
        }
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

        let vfs: &mut ffi::sqlite3_vfs = p_vfs.as_mut().unwrap();
        let mut state: std::ptr::NonNull<State<V>> =
            NonNull::new(vfs.pAppData as _).expect("pAppData is null");
        let state = state.as_mut();

        let slice = CStr::from_ptr(z_path);
        let osstr = OsStr::from_bytes(slice.to_bytes());

        match state.vfs.delete(osstr.as_ref()) {
            Ok(_) => ffi::SQLITE_OK,
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    ffi::SQLITE_OK
                } else {
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

        let vfs: &mut ffi::sqlite3_vfs = p_vfs.as_mut().unwrap();
        let mut state: std::ptr::NonNull<State<V>> =
            NonNull::new(vfs.pAppData as _).expect("pAppData is null");
        let state = state.as_mut();

        let slice = CStr::from_ptr(z_path);
        let osstr = OsStr::from_bytes(slice.to_bytes());

        match state.vfs.access(osstr.as_ref()) {
            Ok(ok) => {
                let p_res_out: &mut c_int = p_res_out.as_mut().unwrap();
                *p_res_out = ok as i32;

                ffi::SQLITE_OK
            }
            Err(_err) => ffi::SQLITE_IOERR_ACCESS,
        }
    }

    /// Populate buffer `z_out` with the full canonical pathname corresponding to the pathname in
    /// `z_path`. `z_out` is guaranteed to point to a buffer of at least (INST_MAX_PATHNAME+1)
    /// bytes.
    pub unsafe extern "C" fn full_pathname(
        _p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        n_out: c_int,
        z_out: *mut c_char,
    ) -> c_int {
        let name = CStr::from_ptr(z_path);
        println!("full_pathname name={}", name.to_string_lossy());
        let name = name.to_bytes_with_nul();
        assert!(name.len() <= n_out as usize); // TODO: proper error
        assert!(name.len() <= MAX_PATH_LENGTH); // TODO: proper error
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

        let msg = CString::new("Loadable extensions are not supported").unwrap();
        ffi::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr());
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
        _p_vfs: *mut ffi::sqlite3_vfs,
        p_time_out: *mut f64,
    ) -> c_int {
        println!("current_time");

        let now = time::OffsetDateTime::now_utc().unix_timestamp() as f64;
        *p_time_out = 2440587.5 + now / 864.0e5;
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn get_last_error(
        _arg1: *mut ffi::sqlite3_vfs,
        _n_byte: c_int,
        _z_err_msg: *mut c_char,
    ) -> c_int {
        todo!("get_last_error")
    }
}

mod io {
    use super::*;

    /// Close a file.
    pub unsafe extern "C" fn close<F>(p_file: *mut ffi::sqlite3_file) -> c_int {
        println!("close");

        let mut f = NonNull::new(p_file as _).unwrap();
        let f: &mut FileState<F> = f.as_mut();

        // TODO: only when free on close is set?
        Box::from_raw(f.file);
        f.file = null_mut();

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

        let mut f = NonNull::new(p_file as _).unwrap();
        let f: &mut FileState<F> = f.as_mut();
        let f: &mut F = f.file.as_mut().unwrap();

        match f.seek(SeekFrom::Start(i_ofst as u64)) {
            Ok(o) => {
                if o != i_ofst as u64 {
                    return ffi::SQLITE_IOERR_READ;
                }
            }
            Err(_) => return ffi::SQLITE_IOERR_READ,
        }

        let out = slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize);
        if let Err(err) = f.read_exact(out) {
            if err.kind() == ErrorKind::UnexpectedEof {
                return ffi::SQLITE_IOERR_SHORT_READ;
            } else {
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

        let mut f = NonNull::new(p_file as _).unwrap();
        let f: &mut FileState<F> = f.as_mut();
        let f: &mut F = f.file.as_mut().unwrap();

        match f.seek(SeekFrom::Start(i_ofst as u64)) {
            Ok(o) => {
                if o != i_ofst as u64 {
                    return ffi::SQLITE_IOERR_WRITE;
                }
            }
            Err(_) => return ffi::SQLITE_IOERR_WRITE,
        }

        let data = slice::from_raw_parts(z as *mut u8, i_amt as usize);
        if f.write_all(data).is_err() {
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

        let mut f = NonNull::new(p_file as _).unwrap();
        let f: &mut FileState<F> = f.as_mut();
        let f: &mut F = f.file.as_mut().unwrap();

        if f.flush().is_err() {
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

        let mut f = NonNull::new(p_file as _).unwrap();
        let f: &mut FileState<F> = f.as_mut();
        let f: &mut F = f.file.as_mut().unwrap();

        let file_size = match f.file_size() {
            Ok(n) => n,
            Err(_) => return ffi::SQLITE_IOERR_FSTAT,
        };
        let p_size: &mut ffi::sqlite3_int64 = p_size.as_mut().unwrap();
        *p_size = file_size as ffi::sqlite3_int64;

        ffi::SQLITE_OK
    }

    /// Lock a file.
    pub unsafe extern "C" fn lock(_p_file: *mut ffi::sqlite3_file, _e_lock: c_int) -> c_int {
        println!("lock");
        // TODO: implement locking
        ffi::SQLITE_OK
    }

    /// Unlock a file.
    pub unsafe extern "C" fn unlock(_p_file: *mut ffi::sqlite3_file, _e_lock: c_int) -> c_int {
        println!("unlock");
        // TODO: implement locking
        ffi::SQLITE_OK
    }

    /// Check if another file-handle holds a RESERVED lock on a file.
    pub unsafe extern "C" fn check_reserved_lock(
        _p_file: *mut ffi::sqlite3_file,
        p_res_out: *mut c_int,
    ) -> c_int {
        println!("check_reserved_lock");

        let p_res_out: &mut c_int = p_res_out.as_mut().unwrap();
        *p_res_out = false as i32;

        // TODO: implement locking
        ffi::SQLITE_OK
    }

    /// File control method. For custom operations on an mem-file.
    pub unsafe extern "C" fn file_control(
        _p_file: *mut ffi::sqlite3_file,
        op: c_int,
        _p_arg: *mut c_void,
    ) -> c_int {
        println!("file_control op={}", op);
        ffi::SQLITE_NOTFOUND
    }

    /// Return the sector-size in bytes for a file.
    pub unsafe extern "C" fn sector_size(_p_file: *mut ffi::sqlite3_file) -> c_int {
        println!("sector_size");

        1
    }

    /// Return the device characteristic flags supported by a file.
    pub unsafe extern "C" fn device_characteristics(_p_file: *mut ffi::sqlite3_file) -> c_int {
        println!("device_characteristics");

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
}

impl<F> Drop for FileState<F> {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.file) };
    }
}

impl File for std::fs::File {
    fn file_size(&self) -> Result<u64, std::io::Error> {
        Ok(self.metadata()?.len())
    }
}
