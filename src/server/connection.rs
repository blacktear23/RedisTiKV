use crate::server::frame::Frame;

use atoi::atoi;
use bytebuffer::ByteBuffer;
use tokio::time::timeout;
use std::io::{self, Cursor, Write};
use crate::server::Result;
use async_std::net::TcpStream;
use async_std::io::{WriteExt, ReadExt, BufReader, BufWriter};

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            r: BufReader::new(socket.clone()),
            w: BufWriter::new(socket),
        }
    }

    pub async fn read_request(&mut self) -> Result<Option<Frame>> {
        let mut start_buf = [0; 4096];
        let size = self.read_line_no_timeout(&mut start_buf).await?;
        // println!("Request First Line {:?}", &start_buf[0..size]);
        if size == 0 || start_buf[0] != b'*' {
            return Err("Invalid Protocol1".into());
        }
        let line = &start_buf[1..size-2];
        let arr_len = atoi::<u64>(line);
        if arr_len.is_none() {
            return Err("Invalid Protocol2".into());
        }
        let mut frame = Frame::array();
        if arr_len.unwrap() == 0 {
            return Ok(None);
        }
        for _i in 0..arr_len.unwrap() {
            match self.read_array_item().await? {
                Frame::Bulk(data) => {
                    frame.push_bulk(data);
                }
                Frame::Integer(data) => {
                    frame.push_int(data);
                }
                Frame::Error(err) => {
                    return Err(err.into());
                }
                _ => {}
            }
        }
        Ok(Some(frame))
    }

    async fn read_array_item(&mut self) -> Result<Frame> {
        let mut item_buf = [0; 4096];
        let size = self.read_line(&mut item_buf).await?;
        if size == 0 {
            return Err("Invalid Protocol3".into());
        }
        match item_buf[0] {
            b'+' => {
                // Simple String
                Ok(Frame::Simple(String::from_utf8_lossy(&item_buf[1..size-2].to_vec()).to_string()))
            }
            b':' => {
                // Integer
                let line = &item_buf[1..size-2];
                let number = atoi::<i64>(line);
                match number {
                    Some(num) => Ok(Frame::Integer(num)),
                    None => Err(format!("protocol error; invalid frame").into()), 
                }
            }
            b'$' => {
                // Bulk String
                let line = &item_buf[1..size-2];
                let bulk_size = atoi::<i64>(line);
                if bulk_size.is_none() {
                    return Err("Invalid Protocol4".into());
                }
                if bulk_size.unwrap() == -1 {
                    Ok(Frame::Null)
                } else {
                    let data = self.read_bulk(bulk_size.unwrap() as usize).await?;
                    Ok(Frame::Bulk(data.into()))
                }
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    async fn read_bulk(&mut self, size: usize) -> Result<Vec<u8>> {
        if size == 0 {
            // Process Zero Size Bulk String
            let mut buf = [0; 2];
            self.read_exact_timeout(&mut buf).await?;
            return Ok("".into());
        }
        let mut buf = vec![0; size+2];
        self.read_exact_timeout(&mut buf).await?;

        Ok(buf[0..size].to_vec())
    }

    async fn read_line_no_timeout(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut rbuf = [0; 1];
        let mut size: usize = 0;
        let mut i = 0;
        loop {
            self.r.read_exact(&mut rbuf).await?;
            size += 1;
            buf[i] = rbuf[0];
            i += 1;
            if rbuf[0] == b'\n' {
                break;
            }
        }
        Ok(size)
    }

    async fn read_line(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut rbuf = [0; 1];
        let mut size: usize = 0;
        let mut i = 0;
        loop {
            self.read_exact_timeout(&mut rbuf).await?;
            size += 1;
            buf[i] = rbuf[0];
            i += 1;
            if rbuf[0] == b'\n' {
                break;
            }
        }
        Ok(size)
    }

    async fn read_exact_timeout(&mut self, buf: &mut [u8]) -> Result<()> {
        let my_duration = tokio::time::Duration::from_millis(500);
        match timeout(my_duration, self.r.read_exact(buf)).await? {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame_buf(&mut self, frame: &Frame) -> io::Result<()> {
        let mut buf = ByteBuffer::new();
        match frame {
            Frame::Array(val) => {
                buf.write_u8(b'*');
                self.write_decimal_buf(&mut buf, val.len() as i64)?;
                for entry in &**val {
                    self.write_value_buf(&mut buf, entry)?;
                }
            }
            _ => self.write_value_buf(&mut buf, frame)?,
        }
        self.w.write_all(&buf.to_bytes()).await?;
        self.w.flush().await
    }

    fn write_decimal_buf(&mut self, buf: &mut ByteBuffer, val: i64) -> io::Result<()> {
        let mut vbuf = [0u8; 20];
        let mut vbuf = Cursor::new(&mut vbuf[..]);
        write!(&mut vbuf, "{}", val)?;
        let pos = vbuf.position() as usize;
        buf.write_all(&vbuf.get_ref()[..pos])?;
        buf.write_all(b"\r\n")?;
        Ok(())
    }

    fn write_value_buf(&mut self, buf: &mut ByteBuffer, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                buf.write_u8(b'+');
                buf.write_all(val.as_bytes())?;
                buf.write_all(b"\r\n")?;
            }
            Frame::Error(val) => {
                buf.write_u8(b'-');
                buf.write_all(val.as_bytes())?;
                buf.write_all(b"\r\n")?;
            }
            Frame::Integer(val) => {
                buf.write_u8(b':');
                self.write_decimal_buf(buf, *val)?;
            }
            Frame::Null => {
                buf.write_all(b"$-1\r\n")?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
                buf.write_u8(b'$');
                self.write_decimal_buf(buf, len as i64)?;
                buf.write_all(val)?;
                buf.write_all(b"\r\n")?;
            }
            Frame::Array(_val) => unreachable!(),
        }
        Ok(())
    }
}