#![allow(unused)]

use core::slice;
use std::ffi::OsStr;
use std::ffi::{c_void, CStr, CString};
use std::io::{Read, Seek};
use std::mem;
use std::mem::{size_of, ManuallyDrop};
use std::os::raw::{c_char, c_int};
use std::os::unix::prelude::OsStrExt;
use std::path::Path;
use std::pin::Pin;
use std::ptr::null_mut;
use std::ptr::NonNull;
use std::thread;
use std::time::Duration;

use rusqlite::{ffi, OpenFlags};

pub trait File: Read + Seek {
    fn file_size(&self) -> Result<u64, std::io::Error>;
}

pub trait Vfs {
    type File: File;

    fn open(&self, path: &Path) -> Result<Self::File, std::io::Error>;
    fn delete(&self, path: &Path) -> Result<(), std::io::Error>;

    fn access(&self, path: &Path) -> Result<bool, std::io::Error> {
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
        xWrite: Some(io::write),
        xTruncate: Some(io::truncate),
        xSync: Some(io::sync),
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
const BLOCK_SIZE: usize = 4096;

#[repr(C)]
struct FileState<F> {
    base: ffi::sqlite3_file,
    file: *mut F,
}

mod vfs {
    use std::io::ErrorKind;

    use super::*;

    pub unsafe extern "C" fn open<F: File, V: Vfs<File = F>>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        mut out_file: *mut ffi::sqlite3_file,
        flags: c_int,
        p_out_flags: *mut c_int,
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
                let mut out_file = NonNull::new(out_file as *mut FileState<F>).unwrap();
                let out_file = out_file.as_mut();
                out_file.base.pMethods = &state.io_methods;
                out_file.file = Box::into_raw(Box::new(f));

                ffi::SQLITE_OK
            }
            Err(_) => ffi::SQLITE_CANTOPEN,
        }
    }

    pub unsafe extern "C" fn delete<V: Vfs>(
        arg1: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        sync_dir: c_int,
    ) -> c_int {
        let name = if z_name.is_null() {
            None
        } else {
            CStr::from_ptr(z_name).to_str().ok()
        };
        println!("delete z_name={:?}", name);

        let vfs: &mut ffi::sqlite3_vfs = arg1.as_mut().unwrap();
        let mut state: std::ptr::NonNull<State<V>> =
            NonNull::new(vfs.pAppData as _).expect("pAppData is null");
        let state = state.as_mut();

        let slice = CStr::from_ptr(z_name);
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

    pub unsafe extern "C" fn access<V: Vfs>(
        arg1: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        flags: c_int,
        p_res_out: *mut c_int,
    ) -> c_int {
        let name = if z_name.is_null() {
            None
        } else {
            CStr::from_ptr(z_name).to_str().ok()
        };
        println!("access z_name={:?} flags={}", name, flags);

        let vfs: &mut ffi::sqlite3_vfs = arg1.as_mut().unwrap();
        let mut state: std::ptr::NonNull<State<V>> =
            NonNull::new(vfs.pAppData as _).expect("pAppData is null");
        let state = state.as_mut();

        let slice = CStr::from_ptr(z_name);
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

    pub unsafe extern "C" fn full_pathname(
        arg1: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        n_out: c_int,
        z_out: *mut c_char,
    ) -> c_int {
        let name = CStr::from_ptr(z_name);
        println!("full_pathname name={}", name.to_string_lossy());
        let name = name.to_bytes_with_nul();
        assert!(name.len() <= n_out as usize); // TODO: proper error
        let out = slice::from_raw_parts_mut(z_out as *mut u8, name.len());
        out.copy_from_slice(name);

        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn dlopen(
        _arg1: *mut ffi::sqlite3_vfs,
        _z_filename: *const c_char,
    ) -> *mut c_void {
        println!("dlopen");

        null_mut()
    }

    pub unsafe extern "C" fn dlerror(
        _arg1: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) {
        println!("dlerror");

        let msg = CString::new("Loadable extensions are not supported").unwrap();
        ffi::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr());
    }

    pub unsafe extern "C" fn dlsym(
        _arg1: *mut ffi::sqlite3_vfs,
        _arg2: *mut c_void,
        _z_symbol: *const c_char,
    ) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const i8)> {
        println!("dlsym");

        None
    }

    pub unsafe extern "C" fn dlclose(_arg1: *mut ffi::sqlite3_vfs, _arg2: *mut c_void) {
        println!("dlclose");
    }

    /// Write `n_bytes` bytes of good-quality randomness into `z_out`. Return the number of bytes of
    /// randomness obtained.
    pub unsafe extern "C" fn randomness(
        _arg1: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_out: *mut c_char,
    ) -> c_int {
        println!("randomness");

        use rand::Rng;

        let bytes = slice::from_raw_parts_mut(z_out, n_byte as usize);
        rand::thread_rng().fill(bytes);
        bytes.len() as c_int
    }

    /// Sleep for at least the number of microseconds given. Return the approximate number of
    /// microseconds slept for.
    pub unsafe extern "C" fn sleep(_arg1: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
        println!("sleep");

        thread::sleep(Duration::from_micros(microseconds as u64));
        ffi::SQLITE_OK
    }

    /// Returns a Julian Day Number for the current date and time as a floating point value.
    pub unsafe extern "C" fn current_time(_arg1: *mut ffi::sqlite3_vfs, arg2: *mut f64) -> c_int {
        println!("current_time");

        let now = time::OffsetDateTime::now_utc().unix_timestamp() as f64;
        *arg2 = 2440587.5 + now / 864.0e5;
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn get_last_error(
        arg1: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) -> c_int {
        todo!("get_last_error")

        // let msg = CString::new("Loadable extensions are not supported").unwrap();
        // ffi::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr());
    }
}

mod io {
    use std::io::{ErrorKind, SeekFrom};
    use std::ptr::null;

    use super::*;

    pub unsafe extern "C" fn close<F>(arg1: *mut ffi::sqlite3_file) -> c_int {
        println!("close");

        let mut f = NonNull::new(arg1 as _).unwrap();
        let f: &mut FileState<F> = f.as_mut();
        Box::from_raw(f.file);
        f.file = null_mut();

        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn read<F: File>(
        arg1: *mut ffi::sqlite3_file,
        arg2: *mut c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        println!("read offset={} len={}", i_ofst, i_amt);

        let mut f = NonNull::new(arg1 as _).unwrap();
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

        let out = slice::from_raw_parts_mut(arg2 as *mut u8, i_amt as usize);
        if let Err(err) = f.read_exact(out) {
            if err.kind() == ErrorKind::UnexpectedEof {
                return ffi::SQLITE_IOERR_SHORT_READ;
            } else {
                return ffi::SQLITE_IOERR_READ;
            }
        }

        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn write(
        arg1: *mut ffi::sqlite3_file,
        arg2: *const c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        println!("write");
        todo!("write");
    }

    pub unsafe extern "C" fn truncate(
        arg1: *mut ffi::sqlite3_file,
        size: ffi::sqlite3_int64,
    ) -> c_int {
        println!("truncate");
        todo!("truncate");
    }

    pub unsafe extern "C" fn sync(arg1: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
        println!("sync");
        todo!("sync");
    }

    pub unsafe extern "C" fn file_size<F: File>(
        arg1: *mut ffi::sqlite3_file,
        p_size: *mut ffi::sqlite3_int64,
    ) -> c_int {
        println!("file_size");

        let mut f = NonNull::new(arg1 as _).unwrap();
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

    pub unsafe extern "C" fn lock(arg1: *mut ffi::sqlite3_file, arg2: c_int) -> c_int {
        println!("lock");
        // TODO: implement locking
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn unlock(arg1: *mut ffi::sqlite3_file, arg2: c_int) -> c_int {
        println!("unlock");
        // TODO: implement locking
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn check_reserved_lock(
        arg1: *mut ffi::sqlite3_file,
        p_res_out: *mut c_int,
    ) -> c_int {
        println!("check_reserved_lock");

        let p_res_out: &mut c_int = p_res_out.as_mut().unwrap();
        *p_res_out = false as i32;

        // TODO: implement locking
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn file_control(
        arg1: *mut ffi::sqlite3_file,
        op: c_int,
        p_arg: *mut c_void,
    ) -> c_int {
        println!("file_control");
        ffi::SQLITE_OK
    }

    /// Returns the sector size of the device that underlies the file.
    pub unsafe extern "C" fn sector_size(arg1: *mut ffi::sqlite3_file) -> c_int {
        println!("sector_size");

        1
    }

    pub unsafe extern "C" fn device_characteristics(arg1: *mut ffi::sqlite3_file) -> c_int {
        println!("device_characteristics");

        // TODO: evaluate which flags make sense to activate
        // writes of any size are atomic
        ffi::SQLITE_IOCAP_ATOMIC
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
