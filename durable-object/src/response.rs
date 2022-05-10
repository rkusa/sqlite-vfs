use std::io::ErrorKind;

use crate::request::Lock;

#[derive(Debug, PartialEq)]
pub enum Response<'a> {
    Open,
    Delete,
    Exists(bool),
    Lock(Lock),
    Get(&'a [u8]),
    Put,
    Size(u64),
    Truncate,
    Reserved(bool),
    /// The connection either:
    /// - did not hold the correct lock for the request, or
    /// - wasn't initialized with a [Request::Open].
    Denied,
}

impl<'a> Response<'a> {
    pub fn decode(data: &'a [u8]) -> std::io::Result<Self> {
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
            5 => Ok(Response::Get(&data[2..])),
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

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Response::Open => buffer.extend_from_slice(&1u16.to_be_bytes()),
            Response::Delete => buffer.extend_from_slice(&2u16.to_be_bytes()),
            Response::Exists(exists) => {
                buffer.extend_from_slice(&3u16.to_be_bytes());
                buffer.extend_from_slice(&[if *exists { 1 } else { 0 }]);
            }
            Response::Lock(lock) => {
                buffer.extend_from_slice(&4u16.to_be_bytes()); // type
                buffer.extend_from_slice(&(*lock as u16).to_be_bytes()); // lock
            }
            Response::Get(data) => {
                buffer.extend_from_slice(&5u16.to_be_bytes());
                buffer.extend_from_slice(data);
            }
            Response::Put => buffer.extend_from_slice(&6u16.to_be_bytes()),
            Response::Size(len) => {
                buffer.extend_from_slice(&7u16.to_be_bytes());
                buffer.extend_from_slice(&len.to_be_bytes());
            }
            Response::Truncate => buffer.extend_from_slice(&8u16.to_be_bytes()),
            Response::Reserved(reserved) => {
                buffer.extend_from_slice(&9u16.to_be_bytes());
                buffer.extend_from_slice(&[if *reserved { 1 } else { 0 }]);
            }
            Response::Denied => buffer.extend_from_slice(&10u16.to_be_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::request::Lock;
    use crate::response::Response;

    #[test]
    fn test_response_open_encode_decode() {
        let res = Response::Open;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_delete_encode_decode() {
        let res = Response::Delete;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_exists_encode_decode() {
        let res = Response::Exists(true);
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_lock_encode_decode() {
        let res = Response::Lock(Lock::Pending);
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_get_encode_decode() {
        let data: Vec<u8> = std::iter::successors(Some(0u8), |n| n.checked_add(1)).collect();
        let res = Response::Get(&data);
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_put_encode_decode() {
        let res = Response::Put;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_size_encode_decode() {
        let res = Response::Size(42);
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_truncate_encode_decode() {
        let res = Response::Truncate;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_reserved_encode_decode() {
        let res = Response::Reserved(true);
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_denied_encode_decode() {
        let res = Response::Denied;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }
}
