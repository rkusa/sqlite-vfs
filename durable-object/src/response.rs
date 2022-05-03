use std::io::ErrorKind;
use std::mem::size_of;

use crate::connection::{Decode, Encode};
use crate::request::Lock;

#[derive(Debug, PartialEq)]
pub enum Response {
    Open,
    Delete,
    Exists(bool),
    Lock(Lock),
    Get(Vec<u8>),
    Put,
    Size(u64),
    Truncate,
    Reserved(bool),
    /// The connection either:
    /// - did not hold the correct lock for the request, or
    /// - wasn't initialized with a [Request::Open].
    Denied,
}

impl Decode for Response {
    fn decode(data: &[u8]) -> std::io::Result<Self> {
        let type_ = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
        );

        match type_ {
            1 => Ok(Response::Open),
            2 => Ok(Response::Delete),
            3 => Ok(Response::Exists(data.get(2) == Some(&1))),
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
                Ok(Response::Lock(lock))
            }
            5 => Ok(Response::Get(data[2..].to_vec())),
            6 => Ok(Response::Put),
            7 => {
                let len = u64::from_be_bytes(
                    data[2..10]
                        .try_into()
                        .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
                );
                Ok(Response::Size(len))
            }
            8 => Ok(Response::Truncate),
            9 => Ok(Response::Reserved(data.get(2) == Some(&1))),
            10 => Ok(Response::Denied),
            type_ => Err(std::io::Error::new(
                ErrorKind::Other,
                format!("invalid response type `{}`", type_),
            )),
        }
    }
}

impl Encode for Response {
    fn encode(&self) -> Vec<u8> {
        match self {
            Response::Open => 1u16.to_be_bytes().to_vec(),
            Response::Delete => 2u16.to_be_bytes().to_vec(),
            Response::Exists(exists) => {
                let mut d = Vec::with_capacity(size_of::<u16>() + size_of::<u8>());
                d.extend_from_slice(&3u16.to_be_bytes());
                d.extend_from_slice(&[if *exists { 1 } else { 0 }]);
                d
            }
            Response::Lock(lock) => {
                let mut d = Vec::with_capacity(2 * size_of::<u16>());
                d.extend_from_slice(&4u16.to_be_bytes()); // type
                d.extend_from_slice(&(*lock as u16).to_be_bytes()); // lock
                d
            }
            Response::Get(data) => {
                let mut d = Vec::with_capacity(size_of::<u16>() + data.len());
                d.extend_from_slice(&5u16.to_be_bytes());
                d.extend_from_slice(data);
                d
            }
            Response::Put => 6u16.to_be_bytes().to_vec(),
            Response::Size(len) => {
                let mut d = Vec::with_capacity(size_of::<u16>() + size_of::<u8>());
                d.extend_from_slice(&7u16.to_be_bytes());
                d.extend_from_slice(&len.to_be_bytes());
                d
            }
            Response::Truncate => 8u16.to_be_bytes().to_vec(),
            Response::Reserved(reserved) => {
                let mut d = Vec::with_capacity(size_of::<u16>() + size_of::<u8>());
                d.extend_from_slice(&9u16.to_be_bytes());
                d.extend_from_slice(&[if *reserved { 1 } else { 0 }]);
                d
            }
            Response::Denied => 10u16.to_be_bytes().to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::{Decode, Encode};
    use crate::request::Lock;
    use crate::response::Response;

    #[test]
    fn test_response_open_encode_decode() {
        let res = Response::Open;
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_delete_encode_decode() {
        let res = Response::Delete;
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_exists_encode_decode() {
        let res = Response::Exists(true);
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_lock_encode_decode() {
        let res = Response::Lock(Lock::Pending);
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_get_encode_decode() {
        let res = Response::Get(std::iter::successors(Some(0u8), |n| n.checked_add(1)).collect());
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_put_encode_decode() {
        let res = Response::Put;
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_size_encode_decode() {
        let res = Response::Size(42);
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_truncate_encode_decode() {
        let res = Response::Truncate;
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_reserved_encode_decode() {
        let res = Response::Reserved(true);
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_denied_encode_decode() {
        let res = Response::Denied;
        let encoded = res.encode();
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }
}
