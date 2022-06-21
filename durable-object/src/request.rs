use std::io::ErrorKind;
use std::ops::Range;

#[derive(Debug, PartialEq)]
pub enum Request<'a> {
    Open {
        db: &'a str,
    },
    LockWalIndex {
        locks: Range<u8>,
        lock: WalIndexLock,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u16)]
pub enum WalIndexLock {
    None = 1,
    Shared,
    Exclusive,
}

impl<'a> Request<'a> {
    pub fn decode(data: &'a [u8]) -> std::io::Result<Self> {
        let type_ = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
        );

        match type_ {
            1 => Ok(Request::Open {
                db: std::str::from_utf8(&data[2..]).unwrap(),
            }),
            4 => {
                let start = *data.get(2).ok_or(ErrorKind::UnexpectedEof)?;
                let end = *data.get(3).ok_or(ErrorKind::UnexpectedEof)?;
                let lock = u16::from_be_bytes(
                    data[4..6]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                let lock = match lock {
                    1 => WalIndexLock::None,
                    2 => WalIndexLock::Shared,
                    3 => WalIndexLock::Exclusive,
                    lock => {
                        return Err(std::io::Error::new(
                            ErrorKind::Other,
                            format!("invalid lock `{}`", lock),
                        ))
                    }
                };
                Ok(Request::LockWalIndex {
                    locks: Range { start, end },
                    lock,
                })
            }
            type_ => Err(std::io::Error::new(
                ErrorKind::Other,
                format!("invalid request type `{}`", type_),
            )),
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Request::Open { db } => {
                buffer.extend_from_slice(&1u16.to_be_bytes()); // type
                buffer.extend_from_slice(db.as_bytes()); // db path
            }
            Request::LockWalIndex { locks, lock } => {
                buffer.extend_from_slice(&4u16.to_be_bytes()); // type
                buffer.extend_from_slice(&locks.start.to_be_bytes()); // start
                buffer.extend_from_slice(&locks.end.to_be_bytes()); // end
                buffer.extend_from_slice(&(*lock as u16).to_be_bytes()); // lock
            }
        }
    }
}

impl Default for WalIndexLock {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::request::WalIndexLock;

    use super::Request;

    #[test]
    fn test_request_open_encode_decode() {
        let req = Request::Open { db: "test.db" };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }


    #[test]
    fn test_request_lock_wal_index_encode_decode() {
        let req = Request::LockWalIndex {
            locks: Range { start: 2, end: 4 },
            lock: WalIndexLock::Exclusive,
        };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }
}
