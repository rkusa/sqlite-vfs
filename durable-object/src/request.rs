use std::io::ErrorKind;
use std::ops::Range;

#[derive(Debug, PartialEq)]
pub enum Request<'a> {
    Open { access: OpenAccess, db: &'a str },
    Delete { db: &'a str },
    Exists { db: &'a str },
    Lock { lock: Lock },
    Get { src: Range<u64> },
    Put { dst: u64, data: &'a [u8] },
    Size,
    Truncate { len: u64 },
    Reserved,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u16)]
pub enum Lock {
    None = 1,
    Shared,
    Reserved,
    Pending,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenAccess {
    Read = 1,
    Write,
    Create,
    CreateNew,
}

impl<'a> Request<'a> {
    pub fn decode(data: &'a [u8]) -> std::io::Result<Self> {
        let type_ = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
        );

        match type_ {
            1 => {
                let access = u16::from_be_bytes(
                    data[2..4]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                let access = match access {
                    1 => OpenAccess::Read,
                    2 => OpenAccess::Write,
                    3 => OpenAccess::Create,
                    4 => OpenAccess::CreateNew,
                    access => {
                        return Err(std::io::Error::new(
                            ErrorKind::Other,
                            format!("invalid access `{}`", access),
                        ))
                    }
                };
                Ok(Request::Open {
                    access,
                    db: std::str::from_utf8(&data[4..]).unwrap(),
                })
            }
            2 => Ok(Request::Delete {
                db: std::str::from_utf8(&data[2..]).unwrap(),
            }),
            3 => Ok(Request::Exists {
                db: std::str::from_utf8(&data[2..]).unwrap(),
            }),
            4 => {
                let lock = u16::from_be_bytes(
                    data[2..4]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                let lock = match lock {
                    1 => Lock::None,
                    2 => Lock::Shared,
                    3 => Lock::Reserved,
                    4 => Lock::Pending,
                    5 => Lock::Exclusive,
                    lock => {
                        return Err(std::io::Error::new(
                            ErrorKind::Other,
                            format!("invalid lock `{}`", lock),
                        ))
                    }
                };
                Ok(Request::Lock { lock })
            }
            5 => {
                let start = u64::from_be_bytes(
                    data[2..10]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                let end = u64::from_be_bytes(
                    data[10..18]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                Ok(Request::Get {
                    src: Range { start, end },
                })
            }
            6 => {
                let dst = u64::from_be_bytes(
                    data[2..10]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                Ok(Request::Put {
                    dst,
                    data: &data[10..],
                })
            }
            7 => Ok(Request::Size),
            8 => {
                let len = u64::from_be_bytes(
                    data[2..10]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                Ok(Request::Truncate { len })
            }
            9 => Ok(Request::Reserved),
            type_ => Err(std::io::Error::new(
                ErrorKind::Other,
                format!("invalid request type `{}`", type_),
            )),
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Request::Open { access, db } => {
                buffer.extend_from_slice(&1u16.to_be_bytes()); // type
                buffer.extend_from_slice(&(*access as u16).to_be_bytes()); // access
                buffer.extend_from_slice(db.as_bytes()); // db path
            }
            Request::Delete { db } => {
                buffer.extend_from_slice(&2u16.to_be_bytes()); // type
                buffer.extend_from_slice(db.as_bytes()); // db path
            }
            Request::Exists { db } => {
                buffer.extend_from_slice(&3u16.to_be_bytes()); // type
                buffer.extend_from_slice(db.as_bytes()); // db path
            }
            Request::Lock { lock } => {
                buffer.extend_from_slice(&4u16.to_be_bytes()); // type
                buffer.extend_from_slice(&(*lock as u16).to_be_bytes()); // lock
            }
            Request::Get { src } => {
                buffer.extend_from_slice(&5u16.to_be_bytes()); // type
                buffer.extend_from_slice(&src.start.to_be_bytes()); // start
                buffer.extend_from_slice(&src.end.to_be_bytes()); // end
            }
            Request::Put { dst, data } => {
                buffer.extend_from_slice(&6u16.to_be_bytes()); // type
                buffer.extend_from_slice(&dst.to_be_bytes()); // dst
                buffer.extend_from_slice(data); // end
            }
            Request::Size => {
                buffer.extend_from_slice(&7u16.to_be_bytes()); // type
            }
            Request::Truncate { len } => {
                buffer.extend_from_slice(&8u16.to_be_bytes()); // type
                buffer.extend_from_slice(&len.to_be_bytes()); // len
            }
            Request::Reserved => {
                buffer.extend_from_slice(&9u16.to_be_bytes()); // type
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::request::{Lock, OpenAccess};

    use super::Request;

    #[test]
    fn test_request_open_encode_decode() {
        let req = Request::Open {
            access: OpenAccess::CreateNew,
            db: "test.db",
        };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_delete_encode_decode() {
        let req = Request::Delete { db: "test.db" };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_exists_encode_decode() {
        let req = Request::Exists { db: "test.db" };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_lock_encode_decode() {
        for i in 1..5 {
            let req = Request::Lock {
                lock: match i {
                    1 => Lock::None,
                    2 => Lock::Shared,
                    3 => Lock::Reserved,
                    4 => Lock::Pending,
                    5 => Lock::Exclusive,
                    _ => unreachable!(),
                },
            };
            let mut encoded = Vec::new();
            req.encode(&mut encoded);
            assert_eq!(Request::decode(&encoded).unwrap(), req);
        }
    }

    #[test]
    fn test_request_get_encode_decode() {
        let req = Request::Get {
            src: Range {
                start: 64,
                end: 128,
            },
        };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_put_encode_decode() {
        let data: Vec<u8> = std::iter::successors(Some(0u8), |n| n.checked_add(1)).collect();
        let req = Request::Put {
            dst: 32,
            data: &data,
        };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_size_encode_decode() {
        let req = Request::Size;
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_truncate_encode_decode() {
        let req = Request::Truncate { len: 42 };
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_reserved_encode_decode() {
        let req = Request::Reserved;
        let mut encoded = Vec::new();
        req.encode(&mut encoded);
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }
}
