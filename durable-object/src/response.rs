use std::io::ErrorKind;

use crate::request::Lock;

#[derive(Debug, PartialEq)]
pub enum Response<'a> {
    /// The connection either:
    /// - did not hold the correct lock for the request, or
    /// - wasn't initialized with a [Request::Open].
    Denied,
    Open,
    Lock(Lock),
    Reserved(bool),
    GetWalIndex(&'a [u8; 32768]),
    PutWalIndex,
    LockWalIndex,
    DeleteWalIndex,
}

impl<'a> Response<'a> {
    pub fn decode(data: &'a [u8]) -> std::io::Result<Self> {
        let type_ = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
        );

        match type_ {
            0 => Ok(Response::Denied),
            1 => Ok(Response::Open),
            2 => {
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
            3 => Ok(Response::Reserved(data.get(2) == Some(&1))),
            4 => {
                let data = data[2..]
                    .try_into()
                    .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?;
                Ok(Response::GetWalIndex(data))
            }
            5 => Ok(Response::PutWalIndex),
            6 => Ok(Response::LockWalIndex),
            7 => Ok(Response::DeleteWalIndex),
            type_ => Err(std::io::Error::new(
                ErrorKind::Other,
                format!("invalid response type `{}`", type_),
            )),
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Response::Denied => buffer.extend_from_slice(&0u16.to_be_bytes()),
            Response::Open => buffer.extend_from_slice(&1u16.to_be_bytes()),
            Response::Lock(lock) => {
                buffer.extend_from_slice(&2u16.to_be_bytes()); // type
                buffer.extend_from_slice(&(*lock as u16).to_be_bytes()); // lock
            }
            Response::Reserved(reserved) => {
                buffer.extend_from_slice(&3u16.to_be_bytes());
                buffer.extend_from_slice(&[if *reserved { 1 } else { 0 }]);
            }
            Response::GetWalIndex(data) => {
                buffer.extend_from_slice(&4u16.to_be_bytes());
                buffer.extend_from_slice(&data[..]);
            }
            Response::PutWalIndex => buffer.extend_from_slice(&5u16.to_be_bytes()),
            Response::LockWalIndex => buffer.extend_from_slice(&6u16.to_be_bytes()),
            Response::DeleteWalIndex => buffer.extend_from_slice(&7u16.to_be_bytes()),
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
    fn test_response_lock_encode_decode() {
        let res = Response::Lock(Lock::Pending);
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

    #[test]
    fn test_response_get_wal_index_encode_decode() {
        let data = [0; 32768];
        let res = Response::GetWalIndex(&data);
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_put_wal_index_encode_decode() {
        let res = Response::PutWalIndex;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_lock_wal_index_encode_decode() {
        let res = Response::LockWalIndex;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }

    #[test]
    fn test_response_delete_wal_index_encode_decode() {
        let res = Response::DeleteWalIndex;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }
}
