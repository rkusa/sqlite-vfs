use std::io::ErrorKind;
use std::mem::size_of;
use std::ops::Range;

use crate::connection::{Decode, Encode};

#[derive(Debug, PartialEq)]
pub enum Request {
    Open { db: String },
    Delete { db: String },
    Exists { db: String },
    Lock { lock: Lock },
    Get { src: Range<u64> },
    Put { dst: u64, data: Vec<u8> },
    Size,
    Truncate { len: u64 },
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

impl Decode for Request {
    fn decode(data: &[u8]) -> std::io::Result<Self> {
        let type_ = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
        );

        match type_ {
            1 => Ok(Request::Open {
                db: String::from_utf8_lossy(&data[2..]).to_string(),
            }),
            2 => Ok(Request::Delete {
                db: String::from_utf8_lossy(&data[2..]).to_string(),
            }),
            3 => Ok(Request::Exists {
                db: String::from_utf8_lossy(&data[2..]).to_string(),
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
                let data = data[10..].to_vec();
                Ok(Request::Put { dst, data })
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
            type_ => Err(std::io::Error::new(
                ErrorKind::Other,
                format!("invalid request type `{}`", type_),
            )),
        }
    }
}

impl Encode for Request {
    fn encode(&self) -> Vec<u8> {
        match self {
            Request::Open { db } => {
                let mut d = Vec::with_capacity(db.len() + size_of::<u16>());
                d.extend_from_slice(&1u16.to_be_bytes()); // type
                d.extend_from_slice(db.as_bytes()); // db path
                d
            }
            Request::Delete { db } => {
                let mut d = Vec::with_capacity(db.len() + size_of::<u16>());
                d.extend_from_slice(&2u16.to_be_bytes()); // type
                d.extend_from_slice(db.as_bytes()); // db path
                d
            }
            Request::Exists { db } => {
                let mut d = Vec::with_capacity(db.len() + size_of::<u16>());
                d.extend_from_slice(&3u16.to_be_bytes()); // type
                d.extend_from_slice(db.as_bytes()); // db path
                d
            }
            Request::Lock { lock } => {
                let mut d = Vec::with_capacity(2 * size_of::<u16>());
                d.extend_from_slice(&4u16.to_be_bytes()); // type
                d.extend_from_slice(&(*lock as u16).to_be_bytes()); // lock
                d
            }
            Request::Get { src } => {
                let mut d = Vec::with_capacity(size_of::<u16>() + 2 * size_of::<u64>());
                d.extend_from_slice(&5u16.to_be_bytes()); // type
                d.extend_from_slice(&src.start.to_be_bytes()); // start
                d.extend_from_slice(&src.end.to_be_bytes()); // end
                d
            }
            Request::Put { dst, data } => {
                let mut d = Vec::with_capacity(size_of::<u16>() + size_of::<u64>() + data.len());
                d.extend_from_slice(&6u16.to_be_bytes()); // type
                d.extend_from_slice(&dst.to_be_bytes()); // dst
                d.extend_from_slice(data); // end
                d
            }
            Request::Size => {
                let mut d = Vec::with_capacity(size_of::<u16>());
                d.extend_from_slice(&7u16.to_be_bytes()); // type
                d
            }
            Request::Truncate { len } => {
                let mut d = Vec::with_capacity(size_of::<u16>() + size_of::<u64>());
                d.extend_from_slice(&8u16.to_be_bytes()); // type
                d.extend_from_slice(&len.to_be_bytes()); // len
                d
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::connection::{Decode, Encode};
    use crate::request::Lock;

    use super::Request;

    #[test]
    fn test_request_open_encode_decode() {
        let req = Request::Open {
            db: "test.db".to_string(),
        };
        let encoded = req.encode();
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_delete_encode_decode() {
        let req = Request::Delete {
            db: "test.db".to_string(),
        };
        let encoded = req.encode();
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_exists_encode_decode() {
        let req = Request::Exists {
            db: "test.db".to_string(),
        };
        let encoded = req.encode();
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
            let encoded = req.encode();
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
        let encoded = req.encode();
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_put_encode_decode() {
        let req = Request::Put {
            dst: 32,
            data: std::iter::successors(Some(0u8), |n| n.checked_add(1)).collect(),
        };
        let encoded = req.encode();
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_size_encode_decode() {
        let req = Request::Size;
        let encoded = req.encode();
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }

    #[test]
    fn test_request_truncate_encode_decode() {
        let req = Request::Truncate { len: 42 };
        let encoded = req.encode();
        assert_eq!(Request::decode(&encoded).unwrap(), req);
    }
}
