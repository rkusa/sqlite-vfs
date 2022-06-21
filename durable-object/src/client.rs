use std::io::{self, ErrorKind};
use std::ops::Range;
use std::os::unix::net::UnixStream;
use std::path::Path;

use crate::connection::Connection;
use crate::request::{Request, WalIndexLock};
use crate::response::Response;

pub struct Client {
    conn: Connection,
}

impl Client {
    pub fn connect(path: impl AsRef<Path>, db: &str) -> io::Result<Self> {
        let stream = UnixStream::connect(path)?;
        let mut client = Client {
            conn: Connection::new(stream),
        };
        let res = client.send(Request::Open { db })?;
        match res {
            Response::Open => Ok(client),
            Response::Denied => Err(ErrorKind::PermissionDenied.into()),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn get_wal_index(&mut self, region: u32) -> io::Result<[u8; 32768]> {
        let res = self.send(Request::GetWalIndex { region })?;
        match res {
            Response::GetWalIndex(data) => Ok(*data),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn put_wal_index(&mut self, region: u32, data: &[u8; 32768]) -> io::Result<()> {
        let res = self.send(Request::PutWalIndex { region, data })?;
        match res {
            Response::PutWalIndex => Ok(()),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn lock_wal_index(&mut self, locks: Range<u8>, lock: WalIndexLock) -> io::Result<bool> {
        let res = self.send(Request::LockWalIndex { locks, lock })?;
        match res {
            Response::LockWalIndex => Ok(true),
            Response::Denied => Ok(false),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn delete_wal_index(&mut self) -> io::Result<()> {
        let res = self.send(Request::DeleteWalIndex)?;
        if let Response::DeleteWalIndex = res {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ))
        }
    }

    fn send(&mut self, req: Request) -> io::Result<Response> {
        self.conn.send(|data: &mut Vec<u8>| req.encode(data))?;
        log::trace!("sent {:?}", req);

        let res = self
            .conn
            .receive()?
            .ok_or_else(|| io::Error::new(ErrorKind::Interrupted, "connection got closed"))?;
        let res = Response::decode(res)?;
        log::trace!("received {:?}", res);

        Ok(res)
    }
}
