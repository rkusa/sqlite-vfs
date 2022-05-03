use std::io::{self, ErrorKind};
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::Range;

use crate::connection::Connection;
use crate::request::{Lock, Request};
use crate::response::Response;

pub struct Client {
    conn: Connection<Request, Response>,
}

impl Client {
    pub fn connect(addr: impl ToSocketAddrs, db: &str) -> io::Result<Self> {
        let mut client = Client {
            conn: Connection::new(TcpStream::connect(addr)?),
        };
        let res = client.send(Request::Open { db: db.to_string() })?;
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
        let mut client = Client {
            conn: Connection::new(TcpStream::connect(addr)?),
        };
        let res = client.send(Request::Delete { db: db.to_string() })?;
        if res != Response::Delete {
            return Err(io::Error::new(
                ErrorKind::Other,
                "received unexpected response",
            ));
        }

        Ok(())
    }

    pub fn exists(addr: impl ToSocketAddrs, db: &str) -> io::Result<bool> {
        let mut client = Client {
            conn: Connection::new(TcpStream::connect(addr)?),
        };
        let res = client.send(Request::Exists { db: db.to_string() })?;
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

    pub fn get(&mut self, src: Range<u64>) -> io::Result<Vec<u8>> {
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

    pub fn put(&mut self, dst: u64, data: Vec<u8>) -> io::Result<()> {
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

    pub fn truncate(&mut self, len: u64) -> io::Result<()> {
        let res = self.send(Request::Truncate { len })?;
        if res != Response::Truncate {
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

    fn send(&mut self, req: Request) -> io::Result<Response> {
        self.conn.send(req)?;

        let res = self
            .conn
            .receive()?
            .ok_or_else(|| io::Error::new(ErrorKind::Interrupted, "connection got closed"))?;

        Ok(res)
    }
}
