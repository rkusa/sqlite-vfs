// Implementation notes:
// - The following code is on purpose not using `tokio::fs` as simply blocking on file access ended
//   up being faster for this particular use-case (of a VFS only targeted for tests) than offloading
//   file access to worker threads.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::prelude::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::atomic::AtomicUsize;
use std::thread;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::task;

use crate::connection::asynchronous::Connection;
use crate::request::{OpenAccess, WalIndexLock};

use super::request::{Lock, Request};
use super::response::Response;

#[derive(Default)]
pub struct Server {
    next_id: AtomicUsize,
    #[allow(clippy::type_complexity)]
    file_locks: Rc<RefCell<HashMap<PathBuf, Weak<RefCell<FileLockState>>>>>,
    #[allow(clippy::type_complexity)]
    wal_indices: Rc<RefCell<HashMap<PathBuf, Weak<RefCell<WalIndex>>>>>,
}

pub struct FileConnection {
    id: usize,
    path: PathBuf,
    file: File,
    file_ino: u64,
    file_lock: Rc<RefCell<FileLockState>>,
    conn_lock: Lock,
    buffer: Vec<u8>,
    wal_index: Rc<RefCell<WalIndex>>,
    wal_index_lock: HashMap<u8, WalIndexLock>,
}

#[derive(Debug, Clone, Copy)]
enum FileLockState {
    /// The object is shared for reading between `count` locks.
    Read { count: usize },
    /// The object has [Lock::Reserved] lock, so new and existing read locks are still allowed, just
    /// not another [Lock::Reserved] (or write) lock.
    Reserved { count: usize },
    /// The object has a [Lock::Pending] lock, so new read locks are not allowed, and it is awaiting
    /// for the read `count` to get to zero.
    Pending { count: usize },
    /// The object has an [Lock::Exclusive] lock.
    Exclusive,
}

#[derive(Default)]
struct WalIndex {
    data: HashMap<u32, [u8; 32768]>,
    locks: HashMap<u8, WalIndexLockState>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum WalIndexLockState {
    Shared { count: usize },
    Exclusive,
}

struct ServerConnection {
    id: usize,
    inner: Connection,
}

impl Server {
    pub async fn start(self, addr: impl ToSocketAddrs) -> io::Result<()> {
        let server = Rc::new(self);
        let listener = TcpListener::bind(addr).await?;
        log::info!(
            "listening to {} (cwd: {:?})",
            listener.local_addr().unwrap(),
            std::env::current_dir().unwrap()
        );

        let local = task::LocalSet::new();

        // Run the local task set.
        local
            .run_until(async move {
                // accept connections and process them serially
                loop {
                    let (stream, _) = listener.accept().await?;
                    log::trace!("received new client connection");

                    stream.set_nodelay(true)?;

                    let server = server.clone();
                    task::spawn_local(async move {
                        if let Err(err) = server.handle_client(stream).await {
                            log::error!("error in connection: {}", err);
                        }
                    });
                }
            })
            .await
    }

    async fn handle_client(self: Rc<Server>, stream: TcpStream) -> io::Result<()> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut conn = ServerConnection {
            id,
            inner: Connection::new(stream),
        };

        match conn.receive().await? {
            Some(Request::Open { access, db }) => {
                let path = normalize_path(Path::new(&db));
                if path.is_dir() {
                    if matches!(access, OpenAccess::Create | OpenAccess::CreateNew) {
                        conn.send(Response::Denied).await?;
                    }
                    return Ok(());
                }

                // Database file might have been deleted externally (e.g. from tests). This is why
                // the existence is checked and later on used to decide whether to reset certain
                // states.
                let exists = path.is_file();

                let file_lock = {
                    let mut objects = self.file_locks.borrow_mut();
                    match objects.entry(path.clone()) {
                        Entry::Occupied(mut entry) => {
                            // database file got deleted by test, reset its lock states
                            let w = entry.get();
                            if let Some(a) = exists.then(|| w.upgrade()).flatten() {
                                a
                            } else {
                                let a: Rc<_> = Default::default();
                                entry.insert(Rc::downgrade(&a));
                                a
                            }
                        }
                        Entry::Vacant(entry) => {
                            let a: Rc<_> = Default::default();
                            entry.insert(Rc::downgrade(&a));
                            a
                        }
                    }
                };
                let wal_index = {
                    let mut objects = self.wal_indices.borrow_mut();
                    match objects.entry(path.clone()) {
                        Entry::Occupied(mut entry) => {
                            let w = entry.get();
                            if let Some(a) = w.upgrade() {
                                // database file got deleted by test, reset its wal indices
                                if !exists {
                                    let mut wal_index = a.borrow_mut();
                                    wal_index.data.clear();
                                    wal_index.locks.clear();
                                }
                                a
                            } else {
                                let a: Rc<_> = Default::default();
                                entry.insert(Rc::downgrade(&a));
                                a
                            }
                        }
                        Entry::Vacant(entry) => {
                            let a: Rc<_> = Default::default();
                            entry.insert(Rc::downgrade(&a));
                            a
                        }
                    }
                };

                let mut o = OpenOptions::new();
                o.read(true).write(access != OpenAccess::Read);
                match access {
                    OpenAccess::Create => {
                        o.create(true);
                    }
                    OpenAccess::CreateNew => {
                        o.create_new(true);
                    }
                    _ => {}
                }
                let file = match o.open(&path) {
                    Ok(f) => {
                        conn.send(Response::Open).await?;
                        f
                    }
                    Err(_err) => {
                        // log::error!("open error: {}", err);
                        conn.send(Response::Denied).await?;
                        return Ok(());
                    }
                };
                let file_ino = file.metadata()?.ino();

                let file_conn = FileConnection {
                    id,
                    path,
                    file,
                    file_ino,
                    file_lock,
                    conn_lock: Lock::None,
                    buffer: Vec::with_capacity(4096),
                    wal_index,
                    wal_index_lock: Default::default(),
                };

                file_conn.handle(conn).await?;
                Ok(())
            }
            Some(Request::Delete { db }) => {
                let path = normalize_path(Path::new(&db));
                {
                    let mut file_locks = self.file_locks.borrow_mut();
                    file_locks.remove(&path);
                }
                match fs::remove_file(path) {
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        conn.send(Response::Denied).await?
                    }
                    Err(err) => return Err(err),
                    Ok(()) => conn.send(Response::Delete).await?,
                }
                Ok(())
            }
            Some(Request::Exists { db }) => {
                let exists = Path::new(db).is_file();
                conn.send(Response::Exists(exists)).await?;
                Ok(())
            }
            Some(_) => Err(io::Error::new(
                ErrorKind::Other,
                "new connections must be initialized with an open request",
            )),
            None => Ok(()),
        }
    }
}

impl ServerConnection {
    async fn receive<'a>(&'a mut self) -> io::Result<Option<Request<'a>>> {
        let res = self.inner.receive().await?;
        match res {
            Some(res) => {
                let res = Request::decode(res)?;
                log::trace!("{{{}}} received {:?}", self.id, res);
                Ok(Some(res))
            }
            None => Ok(None),
        }
    }

    async fn send<'a>(&'a mut self, req: Response<'a>) -> io::Result<()> {
        self.inner
            .send(|data: &mut Vec<u8>| req.encode(data))
            .await?;
        log::trace!("{{{}}} sent {:?}", self.id, req);
        Ok(())
    }
}

impl FileConnection {
    async fn handle(mut self, mut conn: ServerConnection) -> io::Result<()> {
        while let Some(req) = conn.receive().await? {
            let res = self.handle_request(req).unwrap_or_else(|_err| {
                // log::error!("error while handling request: {}", err);
                Response::Denied
            });
            conn.send(res).await?;
        }

        Ok(())
    }

    fn handle_request(&mut self, req: Request) -> io::Result<Response> {
        match req {
            Request::Open { .. } | Request::Delete { .. } | Request::Exists { .. } => {
                Ok(Response::Denied)
            }
            Request::Lock { lock: to } => {
                log::debug!(
                    "{{{}}} request lock {:?} -> {:?} @ {:?} ({:?})",
                    self.id,
                    self.conn_lock,
                    to,
                    self.file_lock.borrow(),
                    self.path,
                );

                let mut ok = self.lock(to)?;
                if !ok {
                    // The following is a workaround for the `crash.test`. It gives the thread of
                    // a crashed database connection another chance to get scheduled to release any
                    // locks it might have. Production implementations should either live with a
                    // chance of not getting an exclusive lock for a very short amount of time
                    // (until the crashed connection is closed), or should ensure a proper order
                    // of unlocking a crashed connection and locking in another connection.
                    log::trace!("{{{}}} lock failed, sleep and retry", self.id);
                    thread::sleep(Duration::from_millis(1));
                    ok = self.lock(to)?;
                }

                if ok {
                    log::debug!(
                        "{{{}}} lock {:?} granted @ {:?} ({:?})",
                        self.id,
                        self.conn_lock,
                        self.file_lock.borrow(),
                        self.path
                    );
                    Ok(Response::Lock(self.conn_lock))
                } else {
                    log::debug!("{{{}}} lock {:?} denied ({:?})", self.id, to, self.path);
                    Ok(Response::Denied)
                }
            }
            Request::Get { src } => {
                self.file.seek(SeekFrom::Start(src.start))?;

                self.buffer.resize((src.end - src.start) as usize, 0);
                self.buffer.shrink_to((src.end - src.start) as usize);
                let mut offset = 0;
                while offset < self.buffer.len() {
                    match self.file.read(&mut self.buffer[offset..]) {
                        Ok(0) => break,
                        Ok(n) => {
                            offset += n;
                        }
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(e) => return Err(e),
                    }
                }

                self.buffer.resize(offset, 0);
                Ok(Response::Get(&self.buffer))
            }
            Request::Put { dst, data } => {
                self.file.seek(SeekFrom::Start(dst))?;
                self.file.write_all(data)?;
                self.file.flush()?;
                Ok(Response::Put)
            }
            Request::Size => Ok(Response::Size(self.file.metadata()?.len())),
            Request::SetLen { len } => {
                self.file.set_len(len)?;
                Ok(Response::SetLen)
            }
            Request::Reserved => {
                let file_lock = self.file_lock.borrow();
                Ok(Response::Reserved(matches!(
                    &*file_lock,
                    FileLockState::Pending { .. }
                        | FileLockState::Reserved { .. }
                        | FileLockState::Exclusive
                )))
            }
            Request::GetWalIndex { region } => {
                let mut wal_index = self.wal_index.borrow_mut();

                // Some tests rely on the existence of the `.db-shm` file, thus make sure it exists,
                // even though it isn't actually used for anything.
                if wal_index.data.is_empty() {
                    fs::write(
                        self.path.with_extension(format!(
                            "{}-shm",
                            self.path
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .unwrap_or("db")
                        )),
                        "",
                    )?;
                }

                let data = wal_index.data.entry(region).or_insert_with(|| [0; 32768]);
                self.buffer.resize(32768, 0);
                (&mut self.buffer[..32768]).copy_from_slice(&data[..]);
                Ok(Response::GetWalIndex(
                    (&self.buffer[..32768]).try_into().unwrap(),
                ))
            }
            Request::PutWalIndex { region, data } => {
                let mut wal_index = self.wal_index.borrow_mut();
                if let Some(previous) = wal_index.data.get(&region) {
                    if previous == data {
                        // log::error!("{{{}}} unnecessary index write!", self.id);
                    }
                }
                wal_index.data.insert(region, *data);

                // Some tests rely on the size of the `.db-shm` file, thus make sure it grows
                let shm =
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(self.path.with_extension(format!(
                            "{}-shm",
                            self.path
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .unwrap_or("db")
                        )))?;
                let size = wal_index.data.keys().max().cloned().unwrap_or(0) * 32768;
                shm.set_len(size as u64)?;

                Ok(Response::PutWalIndex)
            }
            Request::LockWalIndex { locks, lock: to } => {
                let mut wal_index = self.wal_index.borrow_mut();

                // check whether all locks are available
                for region in locks.clone() {
                    let current = wal_index.locks.entry(region).or_default();
                    let from = self.wal_index_lock.entry(region).or_default();
                    log::debug!(
                        "{{{}}} region={} transition {:?} from {:?} to {:?}",
                        self.id,
                        region,
                        current,
                        *from,
                        to
                    );
                    if transition_wal_index_lock(current, *from, to).is_none() {
                        log::warn!("{{{}}} region={} lock {:?} denied", self.id, region, to);
                        return Ok(Response::Denied);
                    }
                }

                // set all locks
                for region in locks {
                    let current = wal_index.locks.entry(region).or_default();
                    let from = self.wal_index_lock.entry(region).or_default();
                    *current = transition_wal_index_lock(current, *from, to).unwrap();
                    *from = to;
                }

                Ok(Response::LockWalIndex)
            }
            Request::DeleteWalIndex => {
                let mut wal_index = self.wal_index.borrow_mut();
                wal_index.data.clear();
                wal_index.locks.clear();
                fs::remove_file(self.path.with_extension(format!(
                            "{}-shm",
                            self.path
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .unwrap_or("db")
                        )))
                .ok();
                Ok(Response::DeleteWalIndex)
            }
            Request::Moved => {
                let ino = fs::metadata(&self.path).map(|m| m.ino()).unwrap_or(0);
                Ok(Response::Moved(ino == 0 || ino != self.file_ino))
            }
        }
    }

    fn lock(&mut self, to: Lock) -> io::Result<bool> {
        let mut file_lock = self.file_lock.borrow_mut();
        match (*file_lock, self.conn_lock, to) {
            // Increment reader count when adding new shared lock.
            (
                FileLockState::Read { .. } | FileLockState::Reserved { .. },
                Lock::None,
                Lock::Shared,
            ) => {
                file_lock.increment();
                self.conn_lock = to;
                Ok(true)
            }

            // Don't allow new shared locks when there is a pending or exclusive lock.
            (
                FileLockState::Pending { .. } | FileLockState::Exclusive,
                Lock::None,
                Lock::Shared,
            ) => Ok(false),

            // Decrement reader count when removing shared lock.
            (
                FileLockState::Read { .. }
                | FileLockState::Reserved { .. }
                | FileLockState::Pending { .. },
                Lock::Shared,
                Lock::None,
            ) => {
                file_lock.decrement();
                self.conn_lock = to;
                Ok(true)
            }

            // Issue a reserved lock.
            (FileLockState::Read { count }, Lock::Shared, Lock::Reserved) => {
                *file_lock = FileLockState::Reserved { count: count - 1 };
                self.conn_lock = to;
                Ok(true)
            }

            // Return from reserved or pending to shared lock.
            (FileLockState::Reserved { count }, Lock::Reserved, Lock::Shared)
            | (FileLockState::Pending { count }, Lock::Pending, Lock::Shared) => {
                *file_lock = FileLockState::Read { count: count + 1 };
                self.conn_lock = to;
                Ok(true)
            }

            // Return from reserved or pending to none lock.
            (FileLockState::Reserved { count }, Lock::Reserved, Lock::None)
            | (FileLockState::Pending { count }, Lock::Pending, Lock::None) => {
                *file_lock = FileLockState::Read { count };
                self.conn_lock = to;
                Ok(true)
            }

            // Only a single write lock allowed.
            (
                FileLockState::Reserved { .. }
                | FileLockState::Pending { .. }
                | FileLockState::Exclusive,
                Lock::Shared,
                Lock::Reserved | Lock::Exclusive,
            ) => Ok(false),

            // Acquire an exclusive lock.
            (FileLockState::Read { count }, Lock::Shared, Lock::Exclusive)
            | (FileLockState::Reserved { count }, Lock::Reserved, Lock::Exclusive)
            | (FileLockState::Pending { count }, Lock::Pending, Lock::Exclusive) => {
                if matches!(&*file_lock, FileLockState::Read { count: 1 }) || count == 0 {
                    *file_lock = FileLockState::Exclusive;
                    self.conn_lock = Lock::Exclusive;
                    Ok(true)
                } else {
                    *file_lock = FileLockState::Pending {
                        count: if matches!(&*file_lock, FileLockState::Read { .. }) {
                            count - 1 // remove itself
                        } else {
                            count
                        },
                    };
                    self.conn_lock = Lock::Pending;
                    Ok(true)
                }
            }

            // Stop writing.
            (FileLockState::Exclusive, Lock::Exclusive, Lock::Shared) => {
                *file_lock = FileLockState::Read { count: 1 };
                self.conn_lock = to;
                Ok(true)
            }
            (FileLockState::Exclusive, Lock::Exclusive, Lock::None) => {
                *file_lock = FileLockState::Read { count: 0 };
                self.conn_lock = to;
                Ok(true)
            }
            _ => {
                // panic!(
                //     "invalid lock transition ({:?}: {:?} to {:?})",
                //     state, self.conn_lock, to
                // );
                Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "invalid lock transition ({:?}: {:?} to {:?})",
                        file_lock, self.conn_lock, to
                    ),
                ))
            }
        }
    }
}

fn transition_wal_index_lock(
    state: &WalIndexLockState,
    from: WalIndexLock,
    to: WalIndexLock,
) -> Option<WalIndexLockState> {
    match (state, from, to) {
        // no change between from and to
        (_, WalIndexLock::None, WalIndexLock::None)
        | (_, WalIndexLock::Shared, WalIndexLock::Shared)
        | (_, WalIndexLock::Exclusive, WalIndexLock::Exclusive) => Some(*state),

        (WalIndexLockState::Shared { count }, WalIndexLock::None, WalIndexLock::Shared) => {
            Some(WalIndexLockState::Shared { count: count + 1 })
        }

        (WalIndexLockState::Shared { count }, WalIndexLock::None, WalIndexLock::Exclusive) => {
            if *count == 0 {
                Some(WalIndexLockState::Exclusive)
            } else {
                None
            }
        }

        (WalIndexLockState::Shared { count }, WalIndexLock::Shared, WalIndexLock::None) => {
            Some(WalIndexLockState::Shared {
                count: count.saturating_sub(1),
            })
        }

        (WalIndexLockState::Shared { count }, WalIndexLock::Shared, WalIndexLock::Exclusive) => {
            if *count == 1 {
                Some(WalIndexLockState::Exclusive)
            } else {
                None
            }
        }

        (WalIndexLockState::Exclusive, WalIndexLock::Exclusive, WalIndexLock::None) => {
            Some(WalIndexLockState::Shared { count: 0 })
        }
        (WalIndexLockState::Exclusive, WalIndexLock::Exclusive, WalIndexLock::Shared) => {
            Some(WalIndexLockState::Shared { count: 1 })
        }

        // invalid state transition
        (WalIndexLockState::Shared { .. }, WalIndexLock::Exclusive, _)
        | (WalIndexLockState::Exclusive, WalIndexLock::None, WalIndexLock::Shared)
        | (WalIndexLockState::Exclusive, WalIndexLock::None, WalIndexLock::Exclusive)
        | (WalIndexLockState::Exclusive, WalIndexLock::Shared, WalIndexLock::None)
        | (WalIndexLockState::Exclusive, WalIndexLock::Shared, WalIndexLock::Exclusive) => None,
    }
}

impl FileLockState {
    fn increment(&mut self) {
        *self = match *self {
            FileLockState::Read { count } => FileLockState::Read { count: count + 1 },
            FileLockState::Reserved { count } => FileLockState::Reserved { count: count + 1 },
            FileLockState::Pending { count } => FileLockState::Pending { count: count + 1 },
            FileLockState::Exclusive => FileLockState::Exclusive,
        };
    }

    fn decrement(&mut self) {
        *self = match *self {
            FileLockState::Read { count } => FileLockState::Read { count: count - 1 },
            FileLockState::Reserved { count } => FileLockState::Reserved { count: count - 1 },
            FileLockState::Pending { count } => FileLockState::Pending { count: count - 1 },
            FileLockState::Exclusive => FileLockState::Exclusive,
        };
    }
}

impl Default for FileLockState {
    fn default() -> Self {
        Self::Read { count: 0 }
    }
}

impl Drop for FileConnection {
    fn drop(&mut self) {
        if self.conn_lock != Lock::None {
            log::trace!(
                "{{{}}} unlocking on connection close from {:?}",
                self.id,
                self.conn_lock
            );

            // make sure lock is removed once connection got closed
            match self.lock(Lock::None) {
                Ok(true) => {}
                Ok(false) => {
                    log::error!("{{{}}} unlock rejected on connection close", self.id)
                }
                Err(err) => log::error!(
                    "{{{}}} failed to unlock on connection close: {}",
                    self.id,
                    err
                ),
            }
        }

        // let has_lock = self
        //     .wal_index_lock
        //     .iter()
        //     .any(|(_, lock)| *lock != WalIndexLock::None);
        // if has_lock {
        //     log::error!("{{{}}} UNLOCKING ON DROP", self.id);
        //     let (start, end) = {
        //         let wal_index = self.wal_index.lock().unwrap();
        //         let start = wal_index.locks.keys().min().cloned();
        //         let end = wal_index.locks.keys().max().cloned();
        //         (start, end)
        //     };
        //     if let Some((start, end)) = start.zip(end) {
        //         if let Err(err) = self.handle_request(Request::LockWalIndex {
        //             locks: start..end.saturating_add(1),
        //             lock: WalIndexLock::None,
        //         }) {
        //             log::error!(
        //                 "{{{}}} failed to unlock wal index on connection close: {}",
        //                 self.id,
        //                 err
        //             );
        //         }
        //     }
        // }
    }
}

// Source: https://github.com/rust-lang/cargo/blob/7a3b56b4860c0e58dab815549a93198a1c335b64/crates/cargo-util/src/paths.rs#L81
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

impl Default for WalIndexLockState {
    fn default() -> Self {
        WalIndexLockState::Shared { count: 0 }
    }
}
