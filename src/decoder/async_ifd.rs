use std::convert::TryFrom;
use std::future::Future;
use std::str;
use std::{io, mem};

use tokio::io::{AsyncRead, AsyncSeek};

use super::async_stream::SmartAsyncReader;
use super::ifd::{offset_to_bytes, offset_to_sbytes, Entry, Value};
use super::stream::ByteOrder;
use crate::tags::Type;
use crate::{TiffError, TiffFormatError, TiffResult};

use crate::decoder::async_stream::EndianAsyncReader;
use crate::decoder::ifd::Value::{
    Ascii, Byte, Double, Float, Ifd, IfdBig, List, Rational, SRational, Short, Signed, SignedBig,
    Unsigned, UnsignedBig,
};

impl Entry {
    pub async fn async_val<R: AsyncRead + AsyncSeek + Unpin + Send>(
        &self,
        limits: &super::Limits,
        bigtiff: bool,
        reader: &mut SmartAsyncReader<R>,
    ) -> TiffResult<Value> {
        // Case 1: there are no values so we can return immediately.
        if self.count == 0 {
            return Ok(List(Vec::new()));
        }

        let bo = reader.byte_order();

        let tag_size = match self.type_ {
            Type::BYTE | Type::SBYTE | Type::ASCII | Type::UNDEFINED => 1,
            Type::SHORT | Type::SSHORT => 2,
            Type::LONG | Type::SLONG | Type::FLOAT | Type::IFD => 4,
            Type::LONG8
            | Type::SLONG8
            | Type::DOUBLE
            | Type::RATIONAL
            | Type::SRATIONAL
            | Type::IFD8 => 8,
        };

        let value_bytes = match self.count.checked_mul(tag_size) {
            Some(n) => n,
            None => {
                return Err(TiffError::LimitsExceeded);
            }
        };

        // Case 2: there is one value.
        if self.count == 1 {
            // 2a: the value is 5-8 bytes and we're in BigTiff mode.
            if bigtiff && value_bytes > 4 && value_bytes <= 8 {
                return Ok(match self.type_ {
                    Type::LONG8 => UnsignedBig(self.async_r(bo).read_u64().await?),
                    Type::SLONG8 => SignedBig(self.async_r(bo).read_i64().await?),
                    Type::DOUBLE => Double(self.async_r(bo).read_f64().await?),
                    Type::RATIONAL => {
                        let mut r = self.async_r(bo);
                        Rational(r.read_u32().await?, r.read_u32().await?)
                    }
                    Type::SRATIONAL => {
                        let mut r = self.async_r(bo);
                        SRational(r.read_i32().await?, r.read_i32().await?)
                    }
                    Type::IFD8 => IfdBig(self.async_r(bo).read_u64().await?),
                    Type::BYTE
                    | Type::SBYTE
                    | Type::ASCII
                    | Type::UNDEFINED
                    | Type::SHORT
                    | Type::SSHORT
                    | Type::LONG
                    | Type::SLONG
                    | Type::FLOAT
                    | Type::IFD => unreachable!(),
                });
            }

            // 2b: the value is at most 4 bytes or doesn't fit in the offset field.
            return Ok(match self.type_ {
                Type::BYTE => Unsigned(u32::from(self.offset[0])),
                Type::SBYTE => Signed(i32::from(self.offset[0] as i8)),
                Type::UNDEFINED => Byte(self.offset[0]),
                Type::SHORT => Unsigned(u32::from(self.async_r(bo).read_u16().await?)),
                Type::SSHORT => Signed(i32::from(self.async_r(bo).read_i16().await?)),
                Type::LONG => Unsigned(self.async_r(bo).read_u32().await?),
                Type::SLONG => Signed(self.async_r(bo).read_i32().await?),
                Type::FLOAT => Float(self.async_r(bo).read_f32().await?),
                Type::ASCII => {
                    if self.offset[0] == 0 {
                        Ascii("".to_string())
                    } else {
                        return Err(TiffError::FormatError(TiffFormatError::InvalidTag));
                    }
                }
                Type::LONG8 => {
                    reader
                        .goto_offset(self.async_r(bo).read_u32().await?.into())
                        .await?;
                    UnsignedBig(reader.read_u64().await?)
                }
                Type::SLONG8 => {
                    reader
                        .goto_offset(self.async_r(bo).read_u32().await?.into())
                        .await?;
                    SignedBig(reader.read_i64().await?)
                }
                Type::DOUBLE => {
                    reader
                        .goto_offset(self.async_r(bo).read_u32().await?.into())
                        .await?;
                    Double(reader.read_f64().await?)
                }
                Type::RATIONAL => {
                    reader
                        .goto_offset(self.async_r(bo).read_u32().await?.into())
                        .await?;
                    Rational(reader.read_u32().await?, reader.read_u32().await?)
                }
                Type::SRATIONAL => {
                    reader
                        .goto_offset(self.async_r(bo).read_u32().await?.into())
                        .await?;
                    SRational(reader.read_i32().await?, reader.read_i32().await?)
                }
                Type::IFD => Ifd(self.async_r(bo).read_u32().await?),
                Type::IFD8 => {
                    reader
                        .goto_offset(self.async_r(bo).read_u32().await?.into())
                        .await?;
                    IfdBig(reader.read_u64().await?)
                }
            });
        }

        // Case 3: There is more than one value, but it fits in the offset field.
        if value_bytes <= 4 || bigtiff && value_bytes <= 8 {
            match self.type_ {
                Type::BYTE => return offset_to_bytes(self.count as usize, self),
                Type::SBYTE => return offset_to_sbytes(self.count as usize, self),
                Type::ASCII => {
                    let mut buf = vec![0; self.count as usize];
                    self.async_r(bo).async_read_exact(&mut buf).await?;
                    if buf.is_ascii() && buf.ends_with(&[0]) {
                        let v = str::from_utf8(&buf)?;
                        let v = v.trim_matches(char::from(0));
                        return Ok(Ascii(v.into()));
                    } else {
                        return Err(TiffError::FormatError(TiffFormatError::InvalidTag));
                    }
                }
                Type::UNDEFINED => {
                    return Ok(List(
                        self.offset[0..self.count as usize]
                            .iter()
                            .map(|&b| Byte(b))
                            .collect(),
                    ));
                }
                Type::SHORT => {
                    let mut r = self.async_r(bo);
                    let mut v = Vec::new();
                    for _ in 0..self.count {
                        v.push(Short(r.read_u16().await?));
                    }
                    return Ok(List(v));
                }
                Type::SSHORT => {
                    let mut r = self.async_r(bo);
                    let mut v = Vec::new();
                    for _ in 0..self.count {
                        v.push(Signed(i32::from(r.read_i16().await?)));
                    }
                    return Ok(List(v));
                }
                Type::LONG => {
                    let mut r = self.async_r(bo);
                    let mut v = Vec::new();
                    for _ in 0..self.count {
                        v.push(Unsigned(r.read_u32().await?));
                    }
                    return Ok(List(v));
                }
                Type::SLONG => {
                    let mut r = self.async_r(bo);
                    let mut v = Vec::new();
                    for _ in 0..self.count {
                        v.push(Signed(r.read_i32().await?));
                    }
                    return Ok(List(v));
                }
                Type::FLOAT => {
                    let mut r = self.async_r(bo);
                    let mut v = Vec::new();
                    for _ in 0..self.count {
                        v.push(Float(r.read_f32().await?));
                    }
                    return Ok(List(v));
                }
                Type::IFD => {
                    let mut r = self.async_r(bo);
                    let mut v = Vec::new();
                    for _ in 0..self.count {
                        v.push(Ifd(r.read_u32().await?));
                    }
                    return Ok(List(v));
                }
                Type::LONG8
                | Type::SLONG8
                | Type::RATIONAL
                | Type::SRATIONAL
                | Type::DOUBLE
                | Type::IFD8 => {
                    unreachable!()
                }
            }
        }

        // Case 4: there is more than one value, and it doesn't fit in the offset field.
        // match self.type_ {
        //     // TODO check if this could give wrong results
        //     // at a different endianess of file/computer.
        //     Type::BYTE => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             let mut buf = [0; 1];
        //             reader.async_read_exact(&mut buf).await?;
        //             Ok(UnsignedBig(u64::from(buf[0])))
        //         })
        //         .await
        //     }
        //     Type::SBYTE => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(SignedBig(i64::from(reader.read_i8().await?)))
        //         })
        //         .await
        //     }
        //     Type::SHORT => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(UnsignedBig(u64::from(reader.read_u16().await?)))
        //         })
        //         .await
        //     }
        //     Type::SSHORT => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(SignedBig(i64::from(reader.read_i16().await?)))
        //         })
        //         .await
        //     }
        //     Type::LONG => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(Unsigned(reader.read_u32().await?))
        //         })
        //         .await
        //     }
        //     Type::SLONG => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(Signed(reader.read_i32().await?))
        //         })
        //         .await
        //     }
        //     Type::FLOAT => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(Float(reader.read_f32().await?))
        //         })
        //         .await
        //     }
        //     Type::DOUBLE => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(Double(reader.read_f64().await?))
        //         })
        //         .await
        //     }
        //     Type::RATIONAL => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(Rational(reader.read_u32().await?, reader.read_u32().await?))
        //         })
        //         .await
        //     }
        //     Type::SRATIONAL => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(SRational(
        //                 reader.read_i32().await?,
        //                 reader.read_i32().await?,
        //             ))
        //         })
        //         .await
        //     }
        //     Type::LONG8 => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(UnsignedBig(reader.read_u64().await?))
        //         })
        //         .await
        //     }
        //     Type::SLONG8 => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(SignedBig(reader.read_i64().await?))
        //         })
        //         .await
        //     }
        //     Type::IFD => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(Ifd(reader.read_u32().await?))
        //         })
        //         .await
        //     }
        //     Type::IFD8 => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             Ok(IfdBig(reader.read_u64().await?))
        //         })
        //         .await
        //     }
        //     Type::UNDEFINED => {
        //         self.decode_async_offset(self.count, bo, bigtiff, limits, reader, |reader| async {
        //             let mut buf = [0; 1];
        //             reader.async_read_exact(&mut buf).await?;
        //             Ok(Byte(buf[0]))
        //         })
        //         .await
        //     }
        //     Type::ASCII => {
        //         let n = usize::try_from(self.count)?;
        //         if n > limits.decoding_buffer_size {
        //             return Err(TiffError::LimitsExceeded);
        //         }

        //         if bigtiff {
        //             reader
        //                 .goto_offset(self.async_r(bo).read_u64().await?)
        //                 .await?
        //         } else {
        //             reader
        //                 .goto_offset(self.async_r(bo).read_u32().await?.into())
        //                 .await?
        //         }

        //         let mut out = vec![0; n];
        //         reader.async_read_exact(&mut out).await?;
        //         // Strings may be null-terminated, so we trim anything downstream of the null byte
        //         if let Some(first) = out.iter().position(|&b| b == 0) {
        //             out.truncate(first);
        //         }
        //         Ok(Ascii(String::from_utf8(out)?))
        //     }
        // }

        todo!()
    }

    #[inline]
    async fn decode_async_offset<R, F, Fut>(
        &self,
        value_count: u64,
        bo: ByteOrder,
        bigtiff: bool,
        limits: &super::Limits,
        reader: &mut SmartAsyncReader<R>,
        decode_fn: F,
    ) -> TiffResult<Value>
    where
        R: AsyncRead + AsyncSeek + Unpin,
        F: Fn(&mut SmartAsyncReader<R>) -> Fut,
        Fut: Future<Output = TiffResult<Value>>,
    {
        let value_count = usize::try_from(value_count)?;
        if value_count > limits.decoding_buffer_size / mem::size_of::<Value>() {
            return Err(TiffError::LimitsExceeded);
        }

        let mut v = Vec::with_capacity(value_count);

        let offset = if bigtiff {
            self.async_r(bo).read_u64().await?
        } else {
            self.async_r(bo).read_u32().await?.into()
        };
        reader.goto_offset(offset).await?;

        for _ in 0..value_count {
            v.push(decode_fn(reader).await?)
        }
        Ok(List(v))
    }

    /// Returns a mem_reader for the offset/value field
    pub fn async_r(&self, byte_order: ByteOrder) -> SmartAsyncReader<io::Cursor<Vec<u8>>> {
        SmartAsyncReader::wrap(io::Cursor::new(self.offset.to_vec()), byte_order)
    }
}
