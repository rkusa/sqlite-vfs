use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::connection::Connection;

use super::request::{Lock, Request};
use super::response::Response;

#[derive(Default)]
pub struct Server {
    #[allow(clippy::type_complexity)]
    file_locks: Arc<RwLock<HashMap<String, Arc<RwLock<FileLock>>>>>,
}

pub struct FileConnection {
    conn: Connection<Response, Request>,
    file: File,
    file_lock: Arc<RwLock<FileLock>>,
    conn_lock: Lock,
}

#[derive(Debug, Clone, Copy)]
enum FileLock {
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

impl Server {
    pub fn start(self, addr: impl ToSocketAddrs) -> io::Result<()> {
        let server = Arc::new(self);
        let listener = TcpListener::bind(addr)?;
        log::info!(
            "listening to {} (cwd: {:?})",
            listener.local_addr().unwrap(),
            std::env::current_dir().unwrap()
        );

        // accept connections and process them serially
        for stream in listener.incoming() {
            let stream = stream?;
            log::trace!("received new client connection");

            let server = server.clone();
            std::thread::spawn(move || {
                if let Err(err) = server.handle_client(stream) {
                    log::error!("error in connection: {}", err);
                }
            });
        }

        Ok(())
    }

    fn handle_client(self: Arc<Server>, stream: TcpStream) -> io::Result<()> {
        let mut conn = Connection::<Response, Request>::new(stream);

        match conn.receive()? {
            Some(Request::Open { db: path }) => {
                let file_lock = {
                    let mut objects = self.file_locks.write().unwrap();
                    objects.entry(path.clone()).or_default().clone()
                };

                let mut o = fs::OpenOptions::new();
                o.read(true).write(true).create(true);
                // o.read(true).write(opts.access != OpenAccess::Read);
                // match opts.access {
                //     OpenAccess::Create => {
                //         o.create(true);
                //     }
                //     OpenAccess::CreateNew => {
                //         o.create_new(true);
                //     }
                //     _ => {}
                // }
                let f = match o.open(&path) {
                    Ok(f) => {
                        conn.send(Response::Open)?;
                        f
                    }
                    Err(_) => {
                        conn.send(Response::Denied)?;
                        return Ok(());
                    }
                };

                FileConnection::handle(conn, f, file_lock)?;
                Ok(())
            }
            Some(Request::Delete { db }) => {
                let mut file_locks = self.file_locks.write().unwrap();
                file_locks.remove(&db);
                fs::remove_file(db)?;
                conn.send(Response::Delete)?;
                Ok(())
            }
            Some(Request::Exists { db }) => {
                conn.send(Response::Exists(Path::new(&db).is_file()))?;
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

impl FileConnection {
    fn handle(
        conn: Connection<Response, Request>,
        file: File,
        file_lock: Arc<RwLock<FileLock>>,
    ) -> io::Result<()> {
        let mut conn = Self {
            conn,
            file,
            file_lock,
            conn_lock: Lock::None,
        };

        while let Some(req) = conn.conn.receive()? {
            let res = conn.handle_request(req)?;
            conn.conn.send(res)?;
        }

        Ok(())
    }

    fn handle_request(&mut self, req: Request) -> io::Result<Response> {
        match req {
            Request::Open { .. } | Request::Delete { .. } | Request::Exists { .. } => {
                Ok(Response::Denied)
            }
            Request::Lock { lock: to } => {
                if self.lock(to)? {
                    log::trace!("lock {:?} granted", to);
                    Ok(Response::Lock)
                } else {
                    log::trace!("lock {:?} denied", to);
                    Ok(Response::Denied)
                }
            }
            Request::Get { src } => {
                self.file.seek(SeekFrom::Start(src.start))?;

                let mut data = vec![0; (src.end - src.start) as usize];
                match self.file.read_exact(&mut data) {
                    Ok(_) => {}
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => {}
                    Err(err) => return Err(err),
                }

                Ok(Response::Get(data))
            }
            Request::Put { dst, data } => {
                self.file.seek(SeekFrom::Start(dst))?;
                self.file.write_all(&data)?;
                self.file.flush()?;
                Ok(Response::Put)
            }
            Request::Size => Ok(Response::Size(self.file.metadata()?.len())),
            Request::Truncate { len } => {
                self.file.set_len(len)?;
                Ok(Response::Truncate)
            }
        }
    }

    fn lock(&mut self, to: Lock) -> io::Result<bool> {
        let mut file_lock = self.file_lock.write().unwrap();
        match (*file_lock, self.conn_lock, to) {
            // Increment reader count when adding new shared lock.
            (FileLock::Read { .. } | FileLock::Reserved { .. }, Lock::None, Lock::Shared) => {
                file_lock.increment();
                self.conn_lock = to;
                Ok(true)
            }

            // Don't allow new shared locks when there is a pending or exclusive lock.
            (FileLock::Pending { .. } | FileLock::Exclusive, Lock::None, Lock::Shared) => Ok(false),

            // Decrement reader count when removing shared lock.
            (
                FileLock::Read { .. } | FileLock::Reserved { .. } | FileLock::Pending { .. },
                Lock::Shared,
                Lock::None,
            ) => {
                file_lock.decrement();
                self.conn_lock = to;
                Ok(true)
            }

            // Issue a reserved lock.
            (FileLock::Read { count }, Lock::Shared, Lock::Reserved) => {
                *file_lock = FileLock::Reserved { count: count - 1 };
                self.conn_lock = to;
                Ok(true)
            }

            // Return from reserved or pending to shared lock.
            (FileLock::Reserved { count }, Lock::Reserved, Lock::Shared)
            | (FileLock::Pending { count }, Lock::Pending, Lock::Shared) => {
                *file_lock = FileLock::Read { count: count + 1 };
                self.conn_lock = to;
                Ok(true)
            }

            // Return from reserved to none lock.
            (FileLock::Reserved { count }, Lock::Reserved, Lock::None) => {
                *file_lock = FileLock::Read { count };
                self.conn_lock = to;
                Ok(true)
            }

            // Only a single write lock allowed.
            (
                FileLock::Reserved { .. } | FileLock::Pending { .. } | FileLock::Exclusive,
                Lock::Shared,
                Lock::Reserved,
            ) => Ok(false),

            // Acquire an exclusive lock.
            (FileLock::Read { count }, Lock::Shared, Lock::Exclusive)
            | (FileLock::Reserved { count }, Lock::Reserved, Lock::Exclusive)
            | (FileLock::Pending { count }, Lock::Pending, Lock::Exclusive) => {
                if (matches!(&*file_lock, FileLock::Read { .. }) && count == 1) || count == 0 {
                    *file_lock = FileLock::Exclusive;
                    self.conn_lock = Lock::Exclusive;
                    Ok(true)
                } else {
                    *file_lock = FileLock::Pending { count };
                    self.conn_lock = Lock::Pending;
                    Ok(false)
                }
            }

            // Stop writing.
            (FileLock::Exclusive, Lock::Exclusive, Lock::Shared) => {
                *file_lock = FileLock::Read { count: 1 };
                self.conn_lock = to;
                Ok(true)
            }
            (FileLock::Exclusive, Lock::Exclusive, Lock::None) => {
                *file_lock = FileLock::Read { count: 0 };
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

impl FileLock {
    fn increment(&mut self) {
        *self = match *self {
            FileLock::Read { count } => FileLock::Read { count: count + 1 },
            FileLock::Reserved { count } => FileLock::Reserved { count: count + 1 },
            FileLock::Pending { count } => FileLock::Pending { count: count + 1 },
            FileLock::Exclusive => FileLock::Exclusive,
        };
    }

    fn decrement(&mut self) {
        *self = match *self {
            FileLock::Read { count } => FileLock::Read { count: count - 1 },
            FileLock::Reserved { count } => FileLock::Reserved { count: count - 1 },
            FileLock::Pending { count } => FileLock::Pending { count: count - 1 },
            FileLock::Exclusive => FileLock::Exclusive,
        };
    }
}

impl Default for FileLock {
    fn default() -> Self {
        Self::Read { count: 0 }
    }
}

impl Drop for FileConnection {
    fn drop(&mut self) {
        if self.conn_lock != Lock::None {
            // make sure lock is removed once connection got closed
            self.lock(Lock::None).ok();
        }
    }
}
