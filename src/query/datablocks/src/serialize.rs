// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::io::fuse::write::FuseWriter;
use common_arrow::arrow::io::parquet::write::transverse;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_arrow::parquet::write::Version;
use common_arrow::write_parquet_file;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

pub fn serialize_data_blocks_with_compression(
    blocks: Vec<DataBlock>,
    schema: impl AsRef<DataSchema>,
    buf: &mut Vec<u8>,
    compression: CompressionOptions,
) -> Result<(u64, ThriftFileMetaData)> {
    let arrow_schema = schema.as_ref().to_arrow();

    let row_group_write_options = WriteOptions {
        write_statistics: false,
        compression,
        version: Version::V2,
        data_pagesize_limit: None,
    };
    let batches = blocks
        .into_iter()
        .map(Chunk::try_from)
        .collect::<Result<Vec<_>>>()?;

    let encoding_map = |data_type: &ArrowDataType| match data_type {
        ArrowDataType::Dictionary(..) => Encoding::RleDictionary,
        _ => col_encoding(data_type),
    };

    let encodings: Vec<Vec<_>> = arrow_schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect::<Vec<_>>();

    let row_groups = RowGroupIterator::try_new(
        batches.into_iter().map(Ok),
        &arrow_schema,
        row_group_write_options,
        encodings,
    )?;

    use common_arrow::parquet::write::WriteOptions as FileWriteOption;
    let options = FileWriteOption {
        write_statistics: false,
        version: Version::V2,
    };

    match write_parquet_file(buf, row_groups, arrow_schema.clone(), options) {
        Ok(result) => Ok(result),
        Err(cause) => Err(ErrorCode::Internal(format!(
            "write_parquet_file: {:?}",
            cause,
        ))),
    }
}

pub fn serialize_data_blocks(
    blocks: Vec<DataBlock>,
    schema: impl AsRef<DataSchema>,
    buf: &mut Vec<u8>,
) -> Result<(u64, ThriftFileMetaData)> {
    serialize_data_blocks_with_compression(blocks, schema, buf, CompressionOptions::Lz4Raw)
}

fn col_encoding(data_type: &ArrowDataType) -> Encoding {
    match data_type {
        ArrowDataType::Binary
        | ArrowDataType::LargeBinary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
        _ => Encoding::Plain,
    }
}

pub fn serialize_data_blocks_fuse(
    blocks: Vec<DataBlock>,
    schema: impl AsRef<DataSchema>,
    buf: &mut Vec<u8>,
) -> Result<(u64, Vec<common_arrow::arrow::io::fuse::ColumnMeta>)> {
    let arrow_schema = schema.as_ref().to_arrow();

    let mut writer = FuseWriter::new(
        buf,
        arrow_schema.clone(),
        common_arrow::arrow::io::fuse::write::WriteOptions {
            compression: Some(common_arrow::arrow::io::fuse::write::Compression::LZ4),
            max_page_size: Some(8192),
        },
    );

    writer.start()?;

    let batches = blocks
        .into_iter()
        .map(Chunk::try_from)
        .collect::<Result<Vec<_>>>()?;

    for batch in batches.iter().take(1) {
        writer.write(&batch)?;
    }

    writer.finish()?;
    Ok((writer.total_size() as u64, writer.metas))
}
