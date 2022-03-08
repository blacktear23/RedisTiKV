use crate::server::frame::{self, Frame};

use atoi::atoi;
use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use crate::server::Result;

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
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(32 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            // if let Some(frame) = self.parse_frame()? {
            //     return Ok(Some(frame));
            // }
            let mut print_buffer = false;
            match self.parse_frame()? {
                Some(frame) => return Ok(Some(frame)),
                None => {
                    if !self.buffer.is_empty() {
                        println!("Buffer not Empty: {:?}", &self.buffer);
                        print_buffer = true;
                    }
                }
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    if print_buffer {
                        println!("Fucking BUG!");
                        return Err("BUG!".into());
                    }
                    println!("Buffer Empty, return None");
                    return Ok(None);
                } else {
                    println!("Connection reset by peer");
                    return Err("connection reset by peer".into());
                }
            }
            if print_buffer {
                println!("New Buffer: {:?}", &self.buffer);
            }
        }
    }

    pub async fn read_request(&mut self) -> Result<Option<Frame>> {
        let mut start_buf = [0; 4096];
        let size = self.read_line(&mut start_buf).await?;
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
            self.stream.read_exact(&mut buf).await?;
            return Ok("".into());
        }
        let mut buf = vec![0; size+2];
        let len = self.stream.read_exact(&mut buf).await?;
        if len < size {
            return Err("Read size not correct".into());
        }
        Ok(buf[0..size].to_vec())
    }

    async fn read_line(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut rbuf = [0; 1];
        let mut size: usize = 0;
        let mut i = 0;
        loop {
            let len = self.stream.read_exact(&mut rbuf).await?;
            if len == 0 {
                break;
            }
            size += len;
            buf[i] = rbuf[0];
            i += 1;
            if rbuf[0] == b'\n' {
                break;
            }
        }
        Ok(size)
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => {
                Ok(None)
            }
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, mini-redis is not able to encode
        // recursive frame structures. See below for more details.
        match frame {
            Frame::Array(val) => {
                // Encode the frame type prefix. For an array, it is `*`.
                self.stream.write_u8(b'*').await?;

                // Encode the length of the array.
                self.write_decimal(val.len() as i64).await?;

                // Iterate and encode each entry in the array.
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // The frame type is a literal. Encode the value directly.
            _ => self.write_value(frame).await?,
        }

        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await
    }

    /// Write a frame literal to the stream
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as i64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // Encoding an `Array` from within a value cannot be done using a
            // recursive strategy. In general, async fns do not support
            // recursion. Mini-redis has not needed to encode nested arrays yet,
            // so for now it is skipped.
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// Write a decimal frame to the stream
    async fn write_decimal(&mut self, val: i64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}