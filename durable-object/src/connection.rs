use std::io::{self, ErrorKind, Read, Write};
use std::mem::size_of;
use std::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    send_buffer: Vec<u8>,
    recv_buffer: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            send_buffer: Vec::with_capacity(8192),
            recv_buffer: vec![0; 8192],
        }
    }

    pub fn send(&mut self, encode: impl FnOnce(&mut Vec<u8>)) -> io::Result<()> {
        self.send_buffer.shrink_to(8192);
        self.send_buffer.clear();

        encode(&mut self.send_buffer);
        // write length
        self.stream
            .write_all(&((size_of::<u32>() + self.send_buffer.len()) as u32).to_be_bytes())?;
        self.stream.write_all(&self.send_buffer)?;

        Ok(())
    }

    pub fn receive(&mut self) -> io::Result<Option<&[u8]>> {
        self.recv_buffer.resize(8192, 0);

        let mut chunk_len = 0;
        loop {
            let n = self.stream.read(&mut self.recv_buffer[chunk_len..])?;
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

            // make sure to have at least 4 bytes necessary to read the expected message length
            if chunk_len < size_of::<u32>() {
                continue;
            }

            let msg_len = u32::from_be_bytes(self.recv_buffer[0..4].try_into().unwrap()) as usize;
            log::trace!("msg_len={}", msg_len);
            if msg_len > self.recv_buffer.len() {
                self.recv_buffer.resize(msg_len, 0);
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

            return Ok(Some(&self.recv_buffer[4..msg_len]));
        }
    }
}
