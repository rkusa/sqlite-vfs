use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::atomic::AtomicUsize;

use tokio::net::{UnixListener, UnixStream};
use tokio::task;

use crate::connection::asynchronous::Connection;
use crate::request::WalIndexLock;

use super::request::Request;
use super::response::Response;

#[derive(Default)]
pub struct Server {
    next_id: AtomicUsize,
    #[allow(clippy::type_complexity)]
    wal_indices: Rc<RefCell<HashMap<PathBuf, Weak<RefCell<WalIndex>>>>>,
}

pub struct FileConnection {
    id: usize,
    buffer: Vec<u8>,
    wal_index: Rc<RefCell<WalIndex>>,
    wal_index_lock: HashMap<u8, WalIndexLock>,
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
    pub async fn start(self, path: impl AsRef<Path>) -> io::Result<()> {
        let server = Rc::new(self);
        let path = path.as_ref();
        let listener = UnixListener::bind(path)?;
        log::info!(
            "listening on UDS `{:?}` (cwd: {:?})",
            path,
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

    async fn handle_client(self: Rc<Server>, stream: UnixStream) -> io::Result<()> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut conn = ServerConnection {
            id,
            inner: Connection::new(stream),
        };

        match conn.receive().await? {
            Some(Request::Open { db }) => {
                let path = normalize_path(Path::new(&db));

                // Database file might have been deleted externally (e.g. from tests). This is why
                // the existence is checked and later on used to decide whether to reset certain
                // states.
                let exists = path.is_file();

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

                conn.send(Response::Open).await?;

                let file_conn = FileConnection {
                    id,
                    buffer: Vec::with_capacity(4096),
                    wal_index,
                    wal_index_lock: Default::default(),
                };

                file_conn.handle(conn).await?;
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
            let res = self.handle_request(req).await.unwrap_or_else(|_err| {
                // log::error!("error while handling request: {}", err);
                Response::Denied
            });
            conn.send(res).await?;
        }

        Ok(())
    }

    async fn handle_request<'a, 'b>(&'a mut self, req: Request<'b>) -> io::Result<Response<'a>> {
        match req {
            Request::Open { .. } => Ok(Response::Denied),
            Request::GetWalIndex { region } => {
                let mut wal_index = self.wal_index.borrow_mut();
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
                Ok(Response::DeleteWalIndex)
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
