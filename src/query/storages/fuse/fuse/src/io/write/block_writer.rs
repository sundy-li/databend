//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use backon::ExponentialBackoff;
use backon::Retryable;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_datablocks::serialize_data_blocks;
use common_datablocks::serialize_data_blocks_fuse;
use common_datablocks::serialize_data_blocks_with_compression;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ClusterStatistics;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::StatisticsOfColumns;
use opendal::Operator;
use tracing::warn;
use uuid::Uuid;

use crate::index::BlockFilter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;

const DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE: usize = 300 * 1024;
const DEFAULT_BLOCK_WRITE_BUFFER_SIZE: usize = 100 * 1024 * 1024;

pub struct BlockWriter<'a> {
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
}

impl<'a> BlockWriter<'a> {
    pub fn new(
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
    ) -> Self {
        Self {
            location_generator,
            data_accessor,
        }
    }

    pub async fn write(
        &self,
        block: DataBlock,
        col_stats: StatisticsOfColumns,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<BlockMeta> {
        let (location, block_id) = self.location_generator.gen_block_location();

        let data_accessor = &self.data_accessor;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let (bloom_filter_index_size, bloom_filter_index_location) = self
            .build_block_index(data_accessor, &block, block_id)
            .await?;
        let (file_size, file_meta_data) = write_block(block, data_accessor, &location.0).await?;
        let col_metas = util::column_metas_fuse(&file_meta_data)?;
        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
            Some(bloom_filter_index_location),
            bloom_filter_index_size,
        );
        Ok(block_meta)
    }

    pub async fn build_block_index(
        &self,
        data_accessor: &Operator,
        block: &DataBlock,
        block_id: Uuid,
    ) -> Result<(u64, Location)> {
        let bloom_index = BlockFilter::try_create(&[block])?;
        let index_block = bloom_index.filter_block;
        let location = self
            .location_generator
            .block_bloom_index_location(&block_id);
        let mut data = Vec::with_capacity(DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE);
        let index_block_schema = &bloom_index.filter_schema;
        let (size, _) = serialize_data_blocks_with_compression(
            vec![index_block],
            index_block_schema,
            &mut data,
            CompressionOptions::Uncompressed,
        )?;
        write_data(&data, data_accessor, &location.0).await?;
        Ok((size, location))
    }
}

pub async fn write_block(
    block: DataBlock,
    data_accessor: &Operator,
    location: &str,
) -> Result<(u64, Vec<common_arrow::arrow::io::fuse::ColumnMeta>)> {
    let mut buf = Vec::with_capacity(DEFAULT_BLOCK_WRITE_BUFFER_SIZE);
    let schema = block.schema().clone();
    let result = serialize_data_blocks_fuse(vec![block], &schema, &mut buf)?;
    write_data(&buf, data_accessor, location).await?;
    Ok(result)
}

pub async fn write_data(data: &[u8], data_accessor: &Operator, location: &str) -> Result<()> {
    let object = data_accessor.object(location);

    { || object.write(data) }
        .retry(ExponentialBackoff::default().with_jitter())
        .when(|err| err.is_temporary())
        .notify(|err, dur| {
            warn!(
                "fuse table block writer write_data retry after {}s for error {:?}",
                dur.as_secs(),
                err
            )
        })
        .await?;

    Ok(())
}
