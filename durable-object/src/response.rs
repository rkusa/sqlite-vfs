use std::io::ErrorKind;

#[derive(Debug, PartialEq)]
pub enum Response {
    /// The connection either:
    /// - did not hold the correct lock for the request, or
    /// - wasn't initialized with a [Request::Open].
    Denied,
    Open,
    LockWalIndex,
}

impl Response {
    pub fn decode(data: &[u8]) -> std::io::Result<Self> {
        let type_ = u16::from_be_bytes(
            data[0..2]
                .try_into()
                .map_err(|err| std::io::Error::new(ErrorKind::UnexpectedEof, err))?,
        );

        match type_ {
            0 => Ok(Response::Denied),
            1 => Ok(Response::Open),
            4 => Ok(Response::LockWalIndex),
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
            Response::LockWalIndex => buffer.extend_from_slice(&4u16.to_be_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::response::Response;

    #[test]
    fn test_response_open_encode_decode() {
        let res = Response::Open;
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
    fn test_response_lock_wal_index_encode_decode() {
        let res = Response::LockWalIndex;
        let mut encoded = Vec::new();
        res.encode(&mut encoded);
        assert_eq!(Response::decode(&encoded).unwrap(), res);
    }
}
