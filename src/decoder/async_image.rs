use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};
use tracing::debug;

use super::async_stream::{AsyncLZWReader, SmartAsyncReader};
use super::async_tag_reader::AsyncTagReader;
use super::ifd::{Directory, Value};
use super::image::{Image, StripDecodeState, TileAttributes};
use super::stream::ByteOrder;
use super::{fp_predict_f32, fp_predict_f64, ChunkType, DecodingBuffer, Limits};
use crate::tags::{
    CompressionMethod, PhotometricInterpretation, PlanarConfiguration, Predictor, SampleFormat, Tag,
};
use crate::{ColorType, TiffError, TiffFormatError, TiffResult, TiffUnsupportedError};
use std::convert::TryFrom;
use std::sync::Arc;

impl Image {
    pub async fn from_async_reader<R: AsyncRead + AsyncSeek + Unpin + Sync + Send>(
        reader: &mut SmartAsyncReader<R>,
        ifd: Directory,
        limits: &Limits,
        bigtiff: bool,
    ) -> TiffResult<Image> {
        let mut tag_reader = AsyncTagReader {
            limits: limits.clone(),
            ifd: ifd.clone(),
            bigtiff,
        };

        let width = tag_reader
            .require_tag(Tag::ImageWidth, reader)
            .await?
            .into_u32()?;
        let height = tag_reader
            .require_tag(Tag::ImageLength, reader)
            .await?
            .into_u32()?;
        if width == 0 || height == 0 {
            return Err(TiffError::FormatError(TiffFormatError::InvalidDimensions(
                width, height,
            )));
        }

        let photometric_interpretation = tag_reader
            .find_tag(Tag::PhotometricInterpretation, reader)
            .await?
            .map(Value::into_u16)
            .transpose()?
            .and_then(PhotometricInterpretation::from_u16)
            .ok_or(TiffUnsupportedError::UnknownInterpretation)?;

        // Try to parse both the compression method and the number, format, and bits of the included samples.
        // If they are not explicitly specified, those tags are reset to their default values and not carried from previous images.
        let compression_method = match tag_reader.find_tag(Tag::Compression, reader).await? {
            Some(val) => CompressionMethod::from_u16_exhaustive(val.into_u16()?),
            None => CompressionMethod::None,
        };

        let jpeg_tables = if compression_method == CompressionMethod::ModernJPEG
            && ifd.contains_key(&Tag::JPEGTables)
        {
            let vec = tag_reader
                .find_tag(Tag::JPEGTables, reader)
                .await?
                .unwrap()
                .into_u8_vec()?;
            if vec.len() < 2 {
                return Err(TiffError::FormatError(
                    TiffFormatError::InvalidTagValueType(Tag::JPEGTables),
                ));
            }

            Some(Arc::new(vec))
        } else {
            None
        };

        let samples: u16 = tag_reader
            .find_tag(Tag::SamplesPerPixel, reader)
            .await?
            .map(Value::into_u16)
            .transpose()?
            .unwrap_or(1);
        if samples == 0 {
            return Err(TiffFormatError::SamplesPerPixelIsZero.into());
        }

        let sample_format = match tag_reader
            .find_tag_uint_vec(Tag::SampleFormat, reader)
            .await?
        {
            Some(vals) => {
                let sample_format: Vec<_> = vals
                    .into_iter()
                    .map(SampleFormat::from_u16_exhaustive)
                    .collect();

                // TODO: for now, only homogenous formats across samples are supported.
                if !sample_format.windows(2).all(|s| s[0] == s[1]) {
                    return Err(TiffUnsupportedError::UnsupportedSampleFormat(sample_format).into());
                }

                sample_format
            }
            None => vec![SampleFormat::Uint],
        };

        let bits_per_sample: Vec<u8> = tag_reader
            .find_tag_uint_vec(Tag::BitsPerSample, reader)
            .await?
            .unwrap_or_else(|| vec![1]);

        // Technically bits_per_sample.len() should be *equal* to samples, but libtiff also allows
        // it to be a single value that applies to all samples.
        // if bits_per_sample.len() != samples.into() && bits_per_sample.len() != 1 {
        //     return Err(TiffError::FormatError(
        //         TiffFormatError::InconsistentSizesEncountered,
        //     ));
        // }

        // This library (and libtiff) do not support mixed sample formats.
        if bits_per_sample.iter().any(|&b| b != bits_per_sample[0]) {
            return Err(TiffUnsupportedError::InconsistentBitsPerSample(bits_per_sample).into());
        }

        let predictor = tag_reader
            .find_tag(Tag::Predictor, reader)
            .await?
            .map(Value::into_u16)
            .transpose()?
            .map(|p| {
                Predictor::from_u16(p)
                    .ok_or(TiffError::FormatError(TiffFormatError::UnknownPredictor(p)))
            })
            .transpose()?
            .unwrap_or(Predictor::None);

        let planar_config = tag_reader
            .find_tag(Tag::PlanarConfiguration, reader)
            .await?
            .map(Value::into_u16)
            .transpose()?
            .map(|p| {
                PlanarConfiguration::from_u16(p).ok_or(TiffError::FormatError(
                    TiffFormatError::UnknownPlanarConfiguration(p),
                ))
            })
            .transpose()?
            .unwrap_or(PlanarConfiguration::Chunky);

        let planes = match planar_config {
            PlanarConfiguration::Chunky => 1,
            PlanarConfiguration::Planar => samples,
        };

        let chunk_type;
        let chunk_offsets;
        let chunk_bytes;
        let strip_decoder;
        let tile_attributes;
        match (
            ifd.contains_key(&Tag::StripByteCounts),
            ifd.contains_key(&Tag::StripOffsets),
            ifd.contains_key(&Tag::TileByteCounts),
            ifd.contains_key(&Tag::TileOffsets),
        ) {
            (true, true, false, false) => {
                chunk_type = ChunkType::Strip;

                chunk_offsets = tag_reader
                    .find_tag(Tag::StripOffsets, reader)
                    .await?
                    .unwrap()
                    .into_u64_vec()?;
                chunk_bytes = tag_reader
                    .find_tag(Tag::StripByteCounts, reader)
                    .await?
                    .unwrap()
                    .into_u64_vec()?;
                let rows_per_strip = tag_reader
                    .find_tag(Tag::RowsPerStrip, reader)
                    .await?
                    .map(Value::into_u32)
                    .transpose()?
                    .unwrap_or(height);
                strip_decoder = Some(StripDecodeState { rows_per_strip });
                tile_attributes = None;

                if chunk_offsets.len() != chunk_bytes.len()
                    || rows_per_strip == 0
                    || u32::try_from(chunk_offsets.len())?
                        != (height.saturating_sub(1) / rows_per_strip + 1) * planes as u32
                {
                    return Err(TiffError::FormatError(
                        TiffFormatError::InconsistentSizesEncountered,
                    ));
                }
            }
            (false, false, true, true) => {
                chunk_type = ChunkType::Tile;

                let tile_width = usize::try_from(
                    tag_reader
                        .require_tag(Tag::TileWidth, reader)
                        .await?
                        .into_u32()?,
                )?;
                let tile_length = usize::try_from(
                    tag_reader
                        .require_tag(Tag::TileLength, reader)
                        .await?
                        .into_u32()?,
                )?;

                if tile_width == 0 {
                    return Err(TiffFormatError::InvalidTagValueType(Tag::TileWidth).into());
                } else if tile_length == 0 {
                    return Err(TiffFormatError::InvalidTagValueType(Tag::TileLength).into());
                }

                strip_decoder = None;
                tile_attributes = Some(TileAttributes {
                    image_width: usize::try_from(width)?,
                    image_height: usize::try_from(height)?,
                    tile_width,
                    tile_length,
                });
                chunk_offsets = tag_reader
                    .find_tag(Tag::TileOffsets, reader)
                    .await?
                    .unwrap()
                    .into_u64_vec()?;
                chunk_bytes = tag_reader
                    .find_tag(Tag::TileByteCounts, reader)
                    .await?
                    .unwrap()
                    .into_u64_vec()?;

                let tile = tile_attributes.as_ref().unwrap();
                if chunk_offsets.len() != chunk_bytes.len()
                    || chunk_offsets.len()
                        != tile.tiles_down() * tile.tiles_across() * planes as usize
                {
                    return Err(TiffError::FormatError(
                        TiffFormatError::InconsistentSizesEncountered,
                    ));
                }
            }
            (_, _, _, _) => {
                return Err(TiffError::FormatError(
                    TiffFormatError::StripTileTagConflict,
                ))
            }
        };

        Ok(Image {
            ifd: Some(ifd),
            width,
            height,
            bits_per_sample: bits_per_sample[0],
            samples,
            sample_format,
            photometric_interpretation,
            compression_method,
            jpeg_tables,
            predictor,
            chunk_type,
            planar_config,
            strip_decoder,
            tile_attributes,
            chunk_offsets,
            chunk_bytes,
        })
    }

    fn create_async_reader<'r, R: AsyncRead + Unpin + Send + 'r>(
        reader: R,
        photometric_interpretation: PhotometricInterpretation,
        compression_method: CompressionMethod,
        compressed_length: u64,
        jpeg_tables: Option<&[u8]>,
    ) -> TiffResult<Box<dyn AsyncRead + Unpin + Send + 'r>> {
        Ok(match compression_method {
            CompressionMethod::None => Box::new(reader),
            CompressionMethod::LZW => {
                let lzw = AsyncLZWReader::new(reader, usize::try_from(compressed_length)?);

                Box::new(lzw)
            }
            _ => {
                unimplemented!("async compression methods are not yet supported")
            }
        })
    }

    pub async fn expand_async_chunk<R>(
        &self,
        reader: R,
        mut buffer: DecodingBuffer<'_>,
        output_width: usize,
        byte_order: ByteOrder,
        chunk_index: u32,
        limits: &Limits,
    ) -> TiffResult<()>
    where
        R: AsyncRead + Unpin + Send + Sync,
    {
        // Validate that the provided buffer is of the expected type.
        let color_type = self.colortype()?;
        match (color_type, &buffer) {
            (ColorType::RGB(n), _)
            | (ColorType::RGBA(n), _)
            | (ColorType::CMYK(n), _)
            | (ColorType::YCbCr(n), _)
            | (ColorType::Gray(n), _)
                if usize::from(n) == buffer.byte_len() * 8 => {}
            (ColorType::Gray(n), DecodingBuffer::U8(_)) if n < 8 => match self.predictor {
                Predictor::None => {}
                Predictor::Horizontal => {
                    return Err(TiffError::UnsupportedError(
                        TiffUnsupportedError::HorizontalPredictor(color_type),
                    ))
                }
                Predictor::FloatingPoint => {
                    return Err(TiffError::UnsupportedError(
                        TiffUnsupportedError::FloatingPointPredictor(color_type),
                    ));
                }
            },
            (type_, _) => {
                return Err(TiffError::UnsupportedError(
                    TiffUnsupportedError::UnsupportedColorType(type_),
                ))
            }
        }

        // Validate that the predictor is supported for the sample type.
        match (self.predictor, &buffer) {
            (Predictor::Horizontal, DecodingBuffer::F32(_))
            | (Predictor::Horizontal, DecodingBuffer::F64(_)) => {
                return Err(TiffError::UnsupportedError(
                    TiffUnsupportedError::HorizontalPredictor(color_type),
                ));
            }
            (Predictor::FloatingPoint, DecodingBuffer::F32(_))
            | (Predictor::FloatingPoint, DecodingBuffer::F64(_)) => {}
            (Predictor::FloatingPoint, _) => {
                return Err(TiffError::UnsupportedError(
                    TiffUnsupportedError::FloatingPointPredictor(color_type),
                ));
            }
            _ => {}
        }

        let compressed_bytes =
            self.chunk_bytes
                .get(chunk_index as usize)
                .ok_or(TiffError::FormatError(
                    TiffFormatError::InconsistentSizesEncountered,
                ))?;
        if *compressed_bytes > limits.intermediate_buffer_size as u64 {
            return Err(TiffError::LimitsExceeded);
        }

        let byte_len = buffer.byte_len();
        let compression_method = self.compression_method;
        let photometric_interpretation = self.photometric_interpretation;
        let predictor = self.predictor;
        let samples = self.samples_per_pixel();

        let chunk_dims = self.chunk_dimensions()?;
        let data_dims = self.chunk_data_dimensions(chunk_index)?;

        let padding_right = chunk_dims.0 - data_dims.0;

        let mut reader = Self::create_async_reader(
            reader,
            photometric_interpretation,
            compression_method,
            *compressed_bytes,
            self.jpeg_tables.as_deref().map(|a| &**a),
        )?;

        if output_width == data_dims.0 as usize && padding_right == 0 {
            debug!("1");
            let total_samples = data_dims.0 as usize * data_dims.1 as usize * samples;
            debug!("data_dims: {:?}", data_dims);
            debug!("total_samples: {:?}", total_samples);
            let tile = &mut buffer.as_bytes_mut()[..total_samples * byte_len];
            debug!("tile len: {:?}", tile.len());
            reader.read_exact(tile).await?;
            debug!("readed");

            for row in 0..data_dims.1 as usize {
                let row_start = row * output_width * samples;
                let row_end = (row + 1) * output_width * samples;
                let row = buffer.subrange(row_start..row_end);
                super::fix_endianness_and_predict(row, samples, byte_order, predictor);
            }
            if photometric_interpretation == PhotometricInterpretation::WhiteIsZero {
                super::invert_colors(&mut buffer.subrange(0..total_samples), color_type);
            }
        } else if padding_right > 0 && self.predictor == Predictor::FloatingPoint {
            debug!("2");
            // The floating point predictor shuffles the padding bytes into the encoded output, so
            // this case is handled specially when needed.
            let mut encoded = vec![0u8; chunk_dims.0 as usize * samples * byte_len];

            for row in 0..data_dims.1 as usize {
                let row_start = row * output_width * samples;
                let row_end = row_start + data_dims.0 as usize * samples;

                reader.read_exact(&mut encoded).await?;
                match buffer.subrange(row_start..row_end) {
                    DecodingBuffer::F32(buf) => fp_predict_f32(&mut encoded, buf, samples),
                    DecodingBuffer::F64(buf) => fp_predict_f64(&mut encoded, buf, samples),
                    _ => unreachable!(),
                }
                if photometric_interpretation == PhotometricInterpretation::WhiteIsZero {
                    super::invert_colors(&mut buffer.subrange(row_start..row_end), color_type);
                }
            }
        } else {
            debug!("3");
            for row in 0..data_dims.1 as usize {
                let row_start = row * output_width * samples;
                let row_end = row_start + data_dims.0 as usize * samples;

                let row = &mut buffer.as_bytes_mut()[(row_start * byte_len)..(row_end * byte_len)];
                reader.read_exact(row).await?;

                // Skip horizontal padding
                if padding_right > 0 {
                    unimplemented!("padding_right > 0");
                    // let len = u64::try_from(padding_right as usize * samples * byte_len)?;
                    // io::copy(&mut reader.take(len), &mut io::sink())?;
                }

                let mut row = buffer.subrange(row_start..row_end);
                super::fix_endianness_and_predict(row.copy(), samples, byte_order, predictor);
                if photometric_interpretation == PhotometricInterpretation::WhiteIsZero {
                    super::invert_colors(&mut row, color_type);
                }
            }
        }

        Ok(())
    }
}
