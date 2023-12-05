use std::convert::TryFrom;
use std::io::{Read, Seek};

use tokio::io::{AsyncRead, AsyncSeek};

use crate::tags::Tag;
use crate::{TiffError, TiffFormatError, TiffResult};

use super::async_stream::SmartAsyncReader;
use super::ifd::{Directory, Value};
use super::stream::SmartReader;
use super::Limits;

pub struct AsyncTagReader {
    pub ifd: Directory,
    pub limits: Limits,
    pub bigtiff: bool,
}

impl AsyncTagReader {
    pub async fn find_tag<R>(
        &mut self,
        tag: Tag,
        reader: &mut SmartAsyncReader<R>,
    ) -> TiffResult<Option<Value>>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        Ok(match self.ifd.get(&tag) {
            Some(entry) => Some(
                entry
                    .clone()
                    .async_val(&self.limits, self.bigtiff, reader)
                    .await?,
            ),
            None => None,
        })
    }
    pub async fn require_tag<R>(
        &mut self,
        tag: Tag,
        reader: &mut SmartAsyncReader<R>,
    ) -> TiffResult<Value>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        match self.find_tag(tag, reader).await? {
            Some(val) => Ok(val),
            None => Err(TiffError::FormatError(
                TiffFormatError::RequiredTagNotFound(tag),
            )),
        }
    }
    pub async fn find_tag_uint_vec<T: TryFrom<u64>, R>(
        &mut self,
        tag: Tag,
        reader: &mut SmartAsyncReader<R>,
    ) -> TiffResult<Option<Vec<T>>>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        self.find_tag(tag, reader)
            .await?
            .map(|v| v.into_u64_vec())
            .transpose()?
            .map(|v| {
                v.into_iter()
                    .map(|u| {
                        T::try_from(u).map_err(|_| TiffFormatError::InvalidTagValueType(tag).into())
                    })
                    .collect()
            })
            .transpose()
    }
}
