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

use std::fmt::Debug;
use std::sync::Arc;

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::io::write_block;
use databend_query::storages::fuse::io::BlockCompactor;
use databend_query::storages::fuse::io::BlockReader;
use databend_query::storages::fuse::io::BlockStreamWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::DEFAULT_BLOCK_PER_SEGMENT;
use futures::StreamExt;
use futures::TryStreamExt;
use num::Integer;
use opendal::ops::OpRead;
use opendal::ops::OpWrite;
use opendal::Accessor;
use opendal::BytesReader;
use opendal::BytesWriter;
use opendal::Operator;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_fuse_table_block_appender() -> Result<()> {
    let ctx = create_query_context().await?;
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);

    // single segment
    let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1, 2, 3])]);
    let block_stream = futures::stream::iter(vec![Ok(block)]);

    let locs = TableMetaLocationGenerator::with_prefix(".".to_owned());
    let segments = BlockStreamWriter::write_block_stream(
        ctx.clone(),
        Box::pin(block_stream),
        DEFAULT_BLOCK_PER_SEGMENT,
        0,
        locs.clone(),
        None,
    )
    .await?
    .collect::<Vec<_>>()
    .await;

    assert_eq!(segments.len(), 1);
    assert!(
        segments[0].is_ok(),
        "oops, unexpected result: {:?}",
        segments[0]
    );

    // multiple segments
    let number_of_blocks = 30;
    let max_rows_per_block = 3;
    let max_blocks_per_segment = 1;
    let block = DataBlock::create(schema, vec![Series::from_data(vec![1, 2, 3])]);
    let blocks = std::iter::repeat(Ok(block)).take(number_of_blocks);
    let block_stream = futures::stream::iter(blocks);

    let segments = BlockStreamWriter::write_block_stream(
        ctx.clone(),
        Box::pin(block_stream),
        max_rows_per_block,
        max_blocks_per_segment,
        locs.clone(),
        None,
    )
    .await?
    .collect::<Vec<_>>()
    .await;

    let len = num::Integer::div_ceil(&number_of_blocks, &max_blocks_per_segment);
    assert_eq!(segments.len(), len);
    for segment in segments.iter() {
        assert!(segment.is_ok(), "oops, unexpected result: {:?}", segment);
    }

    // empty blocks
    let block_stream = futures::stream::iter(vec![]);
    let segments = BlockStreamWriter::write_block_stream(
        ctx,
        Box::pin(block_stream),
        DEFAULT_BLOCK_PER_SEGMENT,
        0,
        locs,
        None,
    )
    .await?
    .collect::<Vec<_>>()
    .await;

    assert!(segments.is_empty());
    Ok(())
}

#[test]
fn test_block_compactor() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let gen_rows = |n| std::iter::repeat(1i32).take(n).collect::<Vec<_>>();
    let gen_block = |col| DataBlock::create(schema.clone(), vec![Series::from_data(col)]);
    let test_case =
        |rows_per_sample_block, max_row_per_block, num_blocks, case_name| -> Result<()> {
            // One block, contains `rows_per_sample_block` rows
            let sample_block = gen_block(gen_rows(rows_per_sample_block));

            let mut compactor = BlockCompactor::new(max_row_per_block);
            let total_rows = rows_per_sample_block * num_blocks;

            let mut generated: Vec<DataBlock> = vec![];

            // feed blocks into shaper
            for _i in 0..num_blocks {
                let blks = compactor.compact(sample_block.clone())?;
                generated.extend(blks.into_iter().flatten())
            }

            // indicates the shaper that we are done
            let sealed = compactor.finish()?;

            // Invariants of `generated` Blocks
            //
            //  1.) row numbers match
            //
            //      generated.iter().map(|b| b.num_rows()).sum() == total_rows
            //
            //  2.) well shaped
            //
            //      - if `max_row_per_block` divides `total_rows`
            //
            //        for all block B in `generated`, B.num_rows() == max_row_per_block,
            //
            //      - if `total_rows` % `max_row_per_block` = r and r > 0
            //
            //        - there should be exactly one block B in `generated` where B.num_rows() == r
            //
            //        - for all block B' in `generated`; B' =/= B implies that B'.row_count() == max_row_per_block,

            let remainder = total_rows % max_row_per_block;
            if remainder == 0 {
                assert!(
                    sealed.is_none(),
                    "[{}], expects no reminder, but got {}",
                    case_name,
                    remainder
                );
            } else {
                assert!(
                    sealed.is_some(),
                    "[{}], expects remainder, but got nothing",
                    case_name
                );
                let block = sealed.as_ref().unwrap();
                assert_eq!(
                    block.iter().map(|i| i.num_rows()).sum::<usize>(),
                    remainder,
                    "[{}], num_rows remains ",
                    case_name
                );
            }

            assert!(
                generated
                    .iter()
                    .all(|item| item.num_rows() == max_row_per_block),
                "[{}], for each block, the num_rows should be exactly max_row_per_block",
                case_name
            );

            generated.extend(sealed.into_iter().flatten());

            assert_eq!(
                total_rows,
                generated.iter().map(|item| item.num_rows()).sum::<usize>(),
                "{}, row numbers match",
                case_name
            );

            Ok(())
        };

    let case_name = "small blocks, with remainder";
    // 3 > 2; 2 * 10 % 3 = 2
    let max_row_per_block = 3;
    let rows_per_sample_block = 2;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    let case_name = "small blocks, no remainder";
    // 4 > 2; 2 * 10 % 4 = 0
    let max_row_per_block = 4;
    let rows_per_sample_block = 2;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    let case_name = "large blocks, no remainder";
    // 15 < 30; 30 * 10 % 15 = 0
    let max_row_per_block = 15;
    let rows_per_sample_block = 30;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    let case_name = "large blocks, with remainders";
    // 7 < 30; 30 * 10 % 7 = 6
    let max_row_per_block = 7;
    let rows_per_sample_block = 30;
    let num_blocks = 10;
    test_case(
        rows_per_sample_block,
        max_row_per_block,
        num_blocks,
        case_name,
    )?;

    Ok(())
}

#[tokio::test]
async fn test_block_stream_writer() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let gen_rows = |n| std::iter::repeat(1i32).take(n).collect::<Vec<_>>();
    let gen_block = |col| DataBlock::create(schema.clone(), vec![Series::from_data(col)]);

    let test_case = |rows_per_sample_block,
                     max_rows_per_block,
                     max_blocks_per_segment,
                     num_blocks,
                     case_name: &'static str| async move {
        let sample_block = gen_block(gen_rows(rows_per_sample_block));
        let block_stream =
            futures::stream::iter(std::iter::repeat(Ok(sample_block)).take(num_blocks));

        let ctx = create_query_context().await?;
        let locs = TableMetaLocationGenerator::with_prefix(".".to_owned());
        let stream = BlockStreamWriter::write_block_stream(
            ctx,
            Box::pin(block_stream),
            max_rows_per_block,
            max_blocks_per_segment,
            locs,
            None,
        )
        .await?;
        let segs = stream.try_collect::<Vec<_>>().await?;

        // verify the number of blocks
        let expected_blocks =
            Integer::div_ceil(&(rows_per_sample_block * num_blocks), &max_rows_per_block);
        let actual_blocks = segs.iter().fold(0, |acc, x| acc + x.blocks.len());
        assert_eq!(expected_blocks, actual_blocks, "case: {}", case_name);

        // verify the number of segment
        let expected_segments = Integer::div_ceil(&expected_blocks, &max_blocks_per_segment);
        assert_eq!(expected_segments, segs.len(), "case: {}", case_name);
        Ok::<_, ErrorCode>(())
    };

    let rows_perf_sample_block = 10;
    let rows_per_block = 10;
    let blocks_per_segment = 10;
    let number_of_blocks = 100;
    test_case(
        rows_perf_sample_block,
        rows_per_block,
        blocks_per_segment,
        number_of_blocks,
        "simple regular data",
    )
    .await?;

    let rows_perf_sample_block = 10;
    let rows_per_block = 3;
    let blocks_per_segment = 3;
    let number_of_blocks = 100;
    test_case(
        rows_perf_sample_block,
        rows_per_block,
        blocks_per_segment,
        number_of_blocks,
        "with remainder",
    )
    .await?;

    Ok(())
}

#[test]
fn test_meta_locations() -> Result<()> {
    let test_prefix = "test_pref";
    let locs = TableMetaLocationGenerator::with_prefix(test_prefix.to_owned());
    let ((path, _ver), _id) = locs.gen_block_location();
    assert!(path.starts_with(test_prefix));
    let seg_loc = locs.gen_segment_info_location();
    assert!(seg_loc.starts_with(test_prefix));
    let uuid = Uuid::new_v4();
    let snapshot_loc = locs.snapshot_location_from_uuid(&uuid, TableSnapshot::VERSION)?;
    assert!(snapshot_loc.starts_with(test_prefix));
    Ok(())
}

#[tokio::test]
async fn test_column_reader_retry_should_return_original_error() -> Result<()> {
    let ctx = create_query_context().await?;
    let operator = ctx.get_storage_operator()?;
    let reader = operator.object("not_exist");
    let r = BlockReader::read_column(reader, 0, 0, 100).await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(ErrorCode::storage_not_found_code(), e.code());
    Ok(())
}

type IOError = std::io::Error;

#[tokio::test]
async fn test_column_reader_retry() -> Result<()> {
    // transient err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::Other,
                "other error",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_other_code(), e.code());
        // read operation should have been retried
        assert!(mock.read_count() > 1);
    }
    // permanent err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "not found",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_not_found_code(), e.code());
        // read operation should have NOT been retried
        assert_eq!(1, mock.read_count());
    }
    Ok(())
}

#[tokio::test]
async fn test_meta_reader_retry() -> Result<()> {
    // transient err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::Other,
                "other error",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_other_code(), e.code());
        // read operation should have been retried
        assert!(mock.read_count() > 1);
    }
    // permanent err
    {
        let errors = std::iter::from_fn(|| {
            Some(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "not found",
            ))
        });
        let mock = Arc::new(Mock::with_exception(errors));
        let op = Operator::new(mock.clone());
        let reader = op.object("does not matter");
        let r = BlockReader::read_column(reader, 0, 0, 1).await;
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert_eq!(ErrorCode::storage_not_found_code(), e.code());
        // read operation should have NOT been retried
        assert_eq!(1, mock.read_count());
    }
    Ok(())
}

#[tokio::test]
async fn test_block_writer_retry() -> Result<()> {
    // transient err
    let errors = std::iter::from_fn(|| {
        Some(std::io::Error::new(
            std::io::ErrorKind::Other,
            "other error",
        ))
    });
    let mock = Arc::new(Mock::with_exception(errors));
    let op = Operator::new(mock.clone());
    let block = DataBlock::empty();
    let r = write_block(block, &op, "loc").await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(ErrorCode::storage_other_code(), e.code());
    // write operation should have been retried
    assert!(mock.write_count() > 1);
    Ok(())
}

#[derive(Debug)]
struct MockState<P> {
    // oneshot err
    errors: P,
    read_op_count: u32,
    write_op_count: u32,
}

impl<P> MockState<P>
where P: Iterator<Item = IOError>
{
    fn with_error(e: P) -> Self {
        Self {
            errors: e,
            read_op_count: 0,
            write_op_count: 0,
        }
    }
    fn take_err(&mut self) -> IOError {
        self.errors.next().unwrap()
    }

    fn increase_read_op_count(&mut self) {
        self.read_op_count += 1;
    }
    fn increase_write_op_count(&mut self) {
        self.write_op_count += 1;
    }
}

#[derive(Debug)]
struct Mock<P> {
    // oneshot err
    err: Mutex<MockState<P>>,
}

impl<P> Mock<P>
where P: Iterator<Item = IOError>
{
    fn with_exception(e: P) -> Self {
        Self {
            err: Mutex::new(MockState::with_error(e)),
        }
    }

    fn read_count(&self) -> u32 {
        let v = &*self.err.lock();
        v.read_op_count
    }
    fn write_count(&self) -> u32 {
        let v = &*self.err.lock();
        v.write_op_count
    }
}

#[async_trait::async_trait]
impl<P> Accessor for Mock<P>
where
    P: Debug + Send,
    P: Iterator<Item = IOError>,
{
    async fn read(&self, _args: &OpRead) -> std::io::Result<BytesReader> {
        let v = &mut (*self.err.lock());
        let err = v.take_err();
        v.increase_read_op_count();
        Err(err)
    }

    async fn write(&self, _args: &OpWrite) -> std::io::Result<BytesWriter> {
        let v = &mut (*self.err.lock());
        let err = v.take_err();
        v.increase_write_op_count();
        Err(err)
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
