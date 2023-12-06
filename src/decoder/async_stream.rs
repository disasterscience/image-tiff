use std::io::{self};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf, Take};
use weezl::decode::Decoder;
use weezl::LzwStatus;

use super::stream::ByteOrder;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

///
/// ## SmartAsyncReader Reader
///

/// Reader that is aware of the byte order.
#[derive(Debug)]
pub struct SmartAsyncReader<R>
where
    R: AsyncRead,
{
    reader: R,
    pub byte_order: ByteOrder,
}

impl<R> SmartAsyncReader<R>
where
    R: AsyncRead,
{
    /// Wraps a reader
    pub fn wrap(reader: R, byte_order: ByteOrder) -> SmartAsyncReader<R> {
        SmartAsyncReader { reader, byte_order }
    }
    pub fn into_inner(self) -> R {
        self.reader
    }
}
impl<R: AsyncRead + AsyncSeek + Unpin> SmartAsyncReader<R> {
    pub async fn goto_offset(&mut self, offset: u64) -> io::Result<()> {
        self.seek(io::SeekFrom::Start(offset)).await.map(|_| ())
    }
}

impl<R> EndianAsyncReader for SmartAsyncReader<R>
where
    R: AsyncRead + Unpin,
{
    #[inline(always)]
    fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }
}

// impl<R: AsyncRead + Unpin> AsyncRead for SmartAsyncReader<R> {
//     // #[inline]
//     // fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//     //     let rd = self.reader.read(buf);

//     //     rd
//     // }

//     fn poll_read(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//         buf: &mut tokio::io::ReadBuf<'_>,
//     ) -> Poll<io::Result<()>> {
//         self.reader.read(buf)
//     }
// }

// impl<R: AsyncRead + AsyncSeek> AsyncSeek for SmartAsyncReader<R> {
//     fn start_seek(self: std::pin::Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
//         todo!()
//     }

//     fn poll_complete(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<io::Result<u64>> {
//         todo!()
//     }
//     // #[inline]
//     // fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
//     //     self.reader.seek(pos)
//     // }
// }

impl<R: AsyncRead + Unpin> AsyncRead for SmartAsyncReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncSeek for SmartAsyncReader<R> {
    fn start_seek(self: std::pin::Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.get_mut().reader).start_seek(position)
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.get_mut().reader).poll_complete(cx)
    }
}

/// Reader that is aware of the byte order.
///

#[async_trait]
pub trait EndianAsyncReader: AsyncRead + Unpin {
    /// Byte order that should be adhered to
    fn byte_order(&self) -> ByteOrder;

    /// Read_exeact interface
    //     fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadExact<'a, Self>
    // where
    //     Self: Unpin,
    // {
    //     read_exact(self, buf)
    // }
    async fn async_read_exact(&mut self, buf: &mut [u8]) -> Result<(), io::Error> {
        self.read_exact(buf).await?;
        Ok(())
    }

    // fn take(self, limit: u64) -> Take<Self>
    // where
    //     Self: Sized,
    // {
    //     self.take(limit)
    // }

    /// Reads an u16
    #[inline(always)]
    async fn read_u16(&mut self) -> Result<u16, io::Error> {
        let mut n = [0u8; 2];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => u16::from_le_bytes(n),
            ByteOrder::BigEndian => u16::from_be_bytes(n),
        })
    }

    /// Reads an i8
    #[inline(always)]
    async fn read_i8(&mut self) -> Result<i8, io::Error> {
        let mut n = [0u8; 1];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => i8::from_le_bytes(n),
            ByteOrder::BigEndian => i8::from_be_bytes(n),
        })
    }

    /// Reads an i16
    #[inline(always)]
    async fn read_i16(&mut self) -> Result<i16, io::Error> {
        let mut n = [0u8; 2];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => i16::from_le_bytes(n),
            ByteOrder::BigEndian => i16::from_be_bytes(n),
        })
    }

    /// Reads an u32
    #[inline(always)]
    async fn read_u32(&mut self) -> Result<u32, io::Error> {
        let mut n = [0u8; 4];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => u32::from_le_bytes(n),
            ByteOrder::BigEndian => u32::from_be_bytes(n),
        })
    }

    /// Reads an i32
    #[inline(always)]
    async fn read_i32(&mut self) -> Result<i32, io::Error> {
        let mut n = [0u8; 4];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => i32::from_le_bytes(n),
            ByteOrder::BigEndian => i32::from_be_bytes(n),
        })
    }

    /// Reads an u64
    #[inline(always)]
    async fn read_u64(&mut self) -> Result<u64, io::Error> {
        let mut n = [0u8; 8];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => u64::from_le_bytes(n),
            ByteOrder::BigEndian => u64::from_be_bytes(n),
        })
    }

    /// Reads an i64
    #[inline(always)]
    async fn read_i64(&mut self) -> Result<i64, io::Error> {
        let mut n = [0u8; 8];
        self.read_exact(&mut n).await?;
        Ok(match self.byte_order() {
            ByteOrder::LittleEndian => i64::from_le_bytes(n),
            ByteOrder::BigEndian => i64::from_be_bytes(n),
        })
    }

    /// Reads an f32
    #[inline(always)]
    async fn read_f32(&mut self) -> Result<f32, io::Error> {
        let mut n = [0u8; 4];
        self.read_exact(&mut n).await?;
        Ok(f32::from_bits(match self.byte_order() {
            ByteOrder::LittleEndian => u32::from_le_bytes(n),
            ByteOrder::BigEndian => u32::from_be_bytes(n),
        }))
    }

    /// Reads an f64
    #[inline(always)]
    async fn read_f64(&mut self) -> Result<f64, io::Error> {
        let mut n = [0u8; 8];
        self.read_exact(&mut n).await?;
        Ok(f64::from_bits(match self.byte_order() {
            ByteOrder::LittleEndian => u64::from_le_bytes(n),
            ByteOrder::BigEndian => u64::from_be_bytes(n),
        }))
    }
}

// /// Reader that decompresses LZW streams
// pub struct LZWReader<R: Read> {
//     reader: BufReader<Take<R>>,
//     decoder: weezl::decode::Decoder,
// }

// impl<R: Read> LZWReader<R> {
//     /// Wraps a reader
//     pub fn new(reader: R, compressed_length: usize) -> LZWReader<R> {
//         Self {
//             reader: BufReader::with_capacity(
//                 (32 * 1024).min(compressed_length),
//                 reader.take(u64::try_from(compressed_length).unwrap()),
//             ),
//             decoder: weezl::decode::Decoder::with_tiff_size_switch(weezl::BitOrder::Msb, 8),
//         }
//     }
// }

// impl<R: Read> Read for LZWReader<R> {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         loop {
//             let result = self.decoder.decode_bytes(self.reader.fill_buf()?, buf);
//             self.reader.consume(result.consumed_in);

//             match result.status {
//                 Ok(weezl::LzwStatus::Ok) => {
//                     if result.consumed_out == 0 {
//                         continue;
//                     } else {
//                         return Ok(result.consumed_out);
//                     }
//                 }
//                 Ok(weezl::LzwStatus::NoProgress) => {
//                     assert_eq!(result.consumed_in, 0);
//                     assert_eq!(result.consumed_out, 0);
//                     assert!(self.reader.buffer().is_empty());
//                     return Err(io::Error::new(
//                         io::ErrorKind::UnexpectedEof,
//                         "no lzw end code found",
//                     ));
//                 }
//                 Ok(weezl::LzwStatus::Done) => {
//                     return Ok(result.consumed_out);
//                 }
//                 Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidData, err)),
//             }
//         }
//     }
// }

pub struct AsyncLZWReader<R: AsyncRead + Unpin> {
    reader: R,
    decoder: Decoder,
    compressed_length: usize,
}

impl<R: AsyncRead + Unpin> AsyncLZWReader<R> {
    pub fn new(reader: R, compressed_length: usize) -> AsyncLZWReader<R> {
        Self {
            reader,
            decoder: weezl::decode::Decoder::with_tiff_size_switch(weezl::BitOrder::Msb, 8),
            compressed_length,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for AsyncLZWReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;

        let mut temp_buf = vec![0; this.compressed_length];
        let mut temp_read_buf = ReadBuf::new(&mut temp_buf);

        match Pin::new(&mut this.reader).poll_read(cx, &mut temp_read_buf) {
            Poll::Ready(Ok(())) => {
                let filled = temp_read_buf.filled();
                let result = this.decoder.decode_bytes(filled, buf.initialize_unfilled());
                match result.status {
                    Ok(LzwStatus::Ok) => {
                        buf.advance(result.consumed_out);
                        Poll::Ready(Ok(()))
                    }
                    Ok(LzwStatus::NoProgress) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "no lzw end code found",
                    ))),
                    Ok(LzwStatus::Done) => {
                        buf.advance(result.consumed_out);
                        Poll::Ready(Ok(()))
                    }
                    Err(err) => Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, err))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
