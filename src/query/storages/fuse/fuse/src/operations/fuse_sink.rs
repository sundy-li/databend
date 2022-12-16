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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_datablocks::serialize_data_blocks;
use common_datablocks::serialize_data_blocks_fuse;
use common_datablocks::serialize_data_blocks_with_compression;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_storages_index::*;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use opendal::Operator;

use super::AppendOperationLogEntry;
use crate::io;
use crate::io::TableMetaLocationGenerator;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::BlockStatistics;
use crate::statistics::ClusterStatsGenerator;
use crate::statistics::StatisticsAccumulator;

pub struct BloomIndexState {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) location: Location,
}

impl BloomIndexState {
    pub fn try_create(
        block: &DataBlock,
        location: Location,
    ) -> Result<(Self, HashMap<usize, usize>)> {
        // write index
        let bloom_index = BlockFilter::try_create(&[block])?;
        let index_block = bloom_index.filter_block;
        let mut data = Vec::with_capacity(100 * 1024);
        let index_block_schema = &bloom_index.filter_schema;
        let (size, _) = serialize_data_blocks_with_compression(
            vec![index_block],
            index_block_schema,
            &mut data,
            CompressionOptions::Uncompressed,
        )?;
        Ok((
            Self {
                data,
                size,
                location,
            },
            bloom_index.column_distinct_count,
        ))
    }
}

enum State {
    None,
    NeedSerialize(DataBlock),
    Serialized {
        data: Vec<u8>,
        size: u64,
        meta_data: Box<Vec<common_arrow::arrow::io::fuse::ColumnMeta>>,
        block_statistics: BlockStatistics,
        bloom_index_state: BloomIndexState,
    },
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    PreCommitSegment {
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Finished,
}

pub struct FuseTableSink {
    state: State,
    input: Arc<InputPort>,
    ctx: Arc<dyn TableContext>,
    data_accessor: Operator,
    num_block_threshold: u64,
    meta_locations: TableMetaLocationGenerator,
    accumulator: StatisticsAccumulator,
    cluster_stats_gen: ClusterStatsGenerator,

    // A dummy output port for distributed insert select to connect Exchange Sink.
    output: Option<Arc<OutputPort>>,
}

impl FuseTableSink {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        num_block_threshold: usize,
        data_accessor: Operator,
        meta_locations: TableMetaLocationGenerator,
        cluster_stats_gen: ClusterStatsGenerator,
        thresholds: BlockCompactThresholds,
        output: Option<Arc<OutputPort>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(FuseTableSink {
            ctx,
            input,
            data_accessor,
            meta_locations,
            state: State::None,
            accumulator: StatisticsAccumulator::new(thresholds),
            num_block_threshold: num_block_threshold as u64,
            cluster_stats_gen,
            output,
        })))
    }
}

#[async_trait]
impl Processor for FuseTableSink {
    fn name(&self) -> String {
        "FuseSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::NeedSerialize(_) | State::GenerateSegment | State::PreCommitSegment { .. }
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::Serialized { .. } | State::SerializedSegment { .. }
        ) {
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            if self.accumulator.summary_row_count != 0 {
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }
            if let Some(output) = &self.output {
                output.finish();
            }
            self.state = State::Finished;
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.state = State::NeedSerialize(self.input.pull_data().unwrap()?);
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::NeedSerialize(data_block) => {
                let (cluster_stats, block) =
                    self.cluster_stats_gen.gen_stats_for_append(&data_block)?;

                let (block_location, block_id) = self.meta_locations.gen_block_location();

                let location = self.meta_locations.block_bloom_index_location(&block_id);
                let (bloom_index_state, column_distinct_count) =
                    BloomIndexState::try_create(&block, location)?;

                let block_statistics = BlockStatistics::from(
                    &block,
                    block_location.0,
                    cluster_stats,
                    Some(column_distinct_count),
                )?;
                // we need a configuration of block size threshold here
                let mut data = Vec::with_capacity(100 * 1024 * 1024);
                let schema = block.schema().clone();
                let (size, meta_data) =
                    serialize_data_blocks_fuse(vec![block], &schema, &mut data)?;

                self.state = State::Serialized {
                    data,
                    size,
                    block_statistics,
                    meta_data: Box::new(meta_data),
                    bloom_index_state,
                };
            }
            State::GenerateSegment => {
                let acc = std::mem::take(&mut self.accumulator);
                let col_stats = acc.summary()?;

                let segment_info = SegmentInfo::new(acc.blocks_metas, Statistics {
                    row_count: acc.summary_row_count,
                    block_count: acc.summary_block_count,
                    perfect_block_count: acc.perfect_block_count,
                    uncompressed_byte_size: acc.in_memory_size,
                    compressed_byte_size: acc.file_size,
                    index_size: acc.index_size,
                    col_stats,
                });

                self.state = State::SerializedSegment {
                    data: serde_json::to_vec(&segment_info)?,
                    location: self.meta_locations.gen_segment_info_location(),
                    segment: Arc::new(segment_info),
                }
            }
            State::PreCommitSegment { location, segment } => {
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    let cache = &mut segment_cache.write();
                    cache.put(location.clone(), segment.clone());
                }

                // TODO: dyn operation for table trait
                let log_entry = AppendOperationLogEntry::new(location, segment);
                let data_block = DataBlock::try_from(log_entry)?;
                self.ctx.push_precommit_block(data_block);
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink"));
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::Serialized {
                data,
                size,
                meta_data,
                block_statistics,
                bloom_index_state,
            } => {
                // write data block
                io::write_data(
                    &data,
                    &self.data_accessor,
                    &block_statistics.block_file_location,
                )
                .await?;

                // write bloom filter index
                io::write_data(
                    &bloom_index_state.data,
                    &self.data_accessor,
                    &bloom_index_state.location.0,
                )
                .await?;

                let bloom_filter_index_size = bloom_index_state.size;
                self.accumulator.add_block(
                    size,
                    *meta_data,
                    block_statistics,
                    Some(bloom_index_state.location),
                    bloom_filter_index_size,
                )?;

                if self.accumulator.summary_block_count >= self.num_block_threshold {
                    self.state = State::GenerateSegment;
                }
            }
            State::SerializedSegment {
                data,
                location,
                segment,
            } => {
                self.data_accessor.object(&location).write(data).await?;

                self.state = State::PreCommitSegment { location, segment };
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink."));
            }
        }

        Ok(())
    }
}
