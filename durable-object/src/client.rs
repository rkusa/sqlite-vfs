use std::io::{self, ErrorKind};
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::Range;

use crate::connection::Connection;
use crate::request::{Lock, OpenAccess, Request, WalIndexLock};
use crate::response::Response;

pub struct Client {
    conn: Connection,
}

impl Client {
    pub fn connect(addr: impl ToSocketAddrs, db: &str, access: OpenAccess) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let mut client = Client {
            conn: Connection::new(stream),
        };
        let res = client.send(Request::Open { access, db })?;
        match res {
            Response::Open => Ok(client),
            Response::Denied => Err(ErrorKind::PermissionDenied.into()),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn delete(addr: impl ToSocketAddrs, db: &str) -> io::Result<()> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let mut client = Client {
            conn: Connection::new(stream),
        };
        let res = client.send(Request::Delete { db })?;
        match res {
            Response::Delete => Ok(()),
            Response::Denied => Err(ErrorKind::NotFound.into()),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn exists(addr: impl ToSocketAddrs, db: &str) -> io::Result<bool> {
        let mut client = Client {
            conn: Connection::new(TcpStream::connect(addr)?),
        };
        let res = client.send(Request::Exists { db })?;
        if let Response::Exists(exists) = res {
            Ok(exists)
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ))
        }
    }

    pub fn lock(&mut self, lock: Lock) -> io::Result<Option<Lock>> {
        let res = self.send(Request::Lock { lock })?;
        match res {
            Response::Lock(lock) => Ok(Some(lock)),
            Response::Denied => Ok(None),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            )),
        }
    }

    pub fn get(&mut self, src: Range<u64>) -> io::Result<&[u8]> {
        let res = self.send(Request::Get { src })?;
        if let Response::Get(data) = res {
            Ok(data)
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ))
        }
    }

    pub fn put(&mut self, dst: u64, data: &[u8]) -> io::Result<()> {
        let res = self.send(Request::Put { dst, data })?;
        if res != Response::Put {
            return Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ));
        }

        Ok(())
    }

    pub fn size(&mut self) -> io::Result<u64> {
        let res = self.send(Request::Size)?;
        if let Response::Size(size) = res {
            Ok(size as u64)
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ))
        }
    }

    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        let res = self.send(Request::SetLen { len })?;
        if res != Response::SetLen {
            return Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ));
        }

        Ok(())
    }

    pub fn is_reserved(&mut self) -> io::Result<bool> {
        let res = self.send(Request::Reserved)?;
        if let Response::Reserved(reserved) = res {
            Ok(reserved)
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ))
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
