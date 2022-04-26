use std::fmt::Debug;
use std::io::{self, ErrorKind, Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::net::TcpStream;

pub struct Connection<Req, Res> {
    stream: TcpStream,
    buffer: Vec<u8>,
    _marker: PhantomData<(Req, Res)>,
}

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode
where
    Self: Sized,
{
    fn decode(data: &[u8]) -> std::io::Result<Self>;
}

impl<Req, Res> Connection<Req, Res>
where
    Req: Encode + Debug,
    Res: Decode + Debug,
{
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: vec![0; 4096],
            _marker: PhantomData,
        }
    }

    pub fn send(&mut self, req: Req) -> io::Result<()> {
        let payload = req.encode();
        // write length
        self.stream
            .write_all(&((size_of::<u16>() + payload.len()) as u16).to_be_bytes())?;
        self.stream.write_all(&payload)?;
        log::trace!("send: {:?}", req);

        Ok(())
    }

    pub fn receive(&mut self) -> io::Result<Option<Res>> {
        let mut chunk_len = 0;
        loop {
            let n = self.stream.read(&mut self.buffer[chunk_len..])?;
            if n == 0 {
                // connection got closed
                if chunk_len > 0 {
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "connection got closed before message was completely received",
                    ));
                } else {
                    return Ok(None);
                }
            }

            chunk_len += n;
            // log::trace!("received data: {:?}", &self.buffer[..chunk_len]);

            // make sure to have at least 2 bytes necessary to read the expected message length
            if chunk_len < 2 {
                continue;
            }

            let msg_len = u16::from_be_bytes([self.buffer[0], self.buffer[1]]) as usize;
            if msg_len > self.buffer.len() {
                self.buffer.resize(msg_len, 0);
                continue;
            }

            if chunk_len < msg_len {
                continue;
            }

            if chunk_len > msg_len {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "excess data (message length: {}; received data: {})",
                        msg_len, chunk_len
                    ),
                ));
            }
            let res = Res::decode(&self.buffer[2..msg_len])?;
            log::trace!("received: {:?}", res);

            self.buffer.resize(4096, 0);

            return Ok(Some(res));
        }
    }
}
