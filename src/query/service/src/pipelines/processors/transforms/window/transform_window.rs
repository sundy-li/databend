// Copyright 2023 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_functions::aggregates::AggregateFunction;
use common_functions::aggregates::StateAddr;
use common_pipeline_transforms::processors::transforms::Transform;

use super::RowPtr;

struct WindowBlock {
    block: DataBlock,
    column_builder: ColumnBuilder,
}

pub struct TransformWindow {
    function: Arc<dyn AggregateFunction>,
    state: StateAddr,

    partition_indices: Vec<usize>,
    order_by_indices: Vec<usize>,

    arena: bumpalo::Bump,
    /// A queue of data blocks that we need to process.
    /// If partition is ended, we may free the data block from front of the queue.
    blocks: VecDeque<WindowBlock>,

    /// monotonically increasing index of the current block in the queue.
    first_block: usize,
    current_block: usize,

    partition_start: RowPtr,
    partition_end: RowPtr,
    partition_ended: bool,

    frame_start: RowPtr,
    frame_end: RowPtr,

    prev_frame_start: RowPtr,
    prev_frame_end: RowPtr,

    frame_started: bool,
    frame_ended: bool,

    current_row: RowPtr,
    input_is_finished: bool,
}

impl TransformWindow {
    fn create() -> Self {
        todo!()
    }

    fn blocks_start(&self) -> RowPtr {
        RowPtr::new(self.first_block, 0)
    }

    fn blocks_end(&self) -> RowPtr {
        RowPtr::new(self.first_block + self.blocks.len() - 1, 0)
    }

    fn block_rows(&self, index: &RowPtr) -> usize {
        self.block_at(index).num_rows()
    }

    fn block_at(&self, index: &RowPtr) -> &DataBlock {
        &self.blocks[index.block - self.first_block].block
    }

    fn column_at(&self, index: &RowPtr, column_index: usize) -> &Column {
        (&self.block_at(index).get_by_offset(column_index).value)
            .as_column()
            .unwrap()
    }

    /// Advance the partition end to the next partition or the end of the data.
    fn advance_partition(&mut self) {
        if self.partition_ended {
            return;
        }

        let end = self.blocks_end();

        if self.input_is_finished {
            self.partition_ended = true;
            assert_eq!(self.partition_end, end);
            return;
        }

        if self.partition_end == end {
            return;
        }

        assert_eq!(end.block, self.partition_end.block + 1);

        let partition_by_columns = self.partition_indices.len();
        if partition_by_columns == 0 {
            self.partition_end = end;
            return;
        }

        assert!(self.partition_start <= self.prev_frame_start);
        assert!(
            self.prev_frame_start < self.partition_end
                || self.partition_start == self.partition_end
        );
        assert!(self.first_block <= self.prev_frame_start.block);

        let block_rows = self.block_rows(&self.partition_end);

        while self.partition_end.row < block_rows {
            let mut i = 0;
            while i < partition_by_columns {
                let prev_column = self.column_at(&self.prev_frame_start, self.partition_indices[i]);
                let compare_column = self.column_at(&self.partition_end, self.partition_indices[i]);

                if prev_column.index(self.prev_frame_start.row)
                    != compare_column.index(self.partition_end.row)
                {
                    break;
                }
                i += 1;
            }

            if i < partition_by_columns {
                self.partition_ended = true;
                return;
            }
            self.partition_end.row += 1;
        }

        assert_eq!(self.partition_end.row, block_rows);
        self.partition_end.block += 1;
        self.partition_end.row = 0;

        assert!(!self.partition_ended && self.partition_end == self.blocks_end());
    }

    fn advance_frame_start(&mut self) {
        if self.frame_started {
            return;
        }
        todo!()
    }

    fn advance_frame_end(&mut self) {
        todo!()
    }

    fn add_block(&mut self, data: DataBlock) -> Result<()> {
        self.blocks.push_back(WindowBlock {
            block: data,
            column_builder: todo!(),
        });

        loop {
            self.advance_partition();

            while self.current_row < self.partition_end {
                self.advance_frame_start();

                // Need more data to make the frame start.
                if !self.frame_started {
                    return;
                }

                self.advance_frame_end();
                if self.frame_ended {
                    break;
                }
            }

            self.advance_frame_start();
            if self.frame_started {
                break;
            }

            self.advance_frame_end();
            if self.frame_ended {
                break;
            }
        }
        todo!()
    }
}

impl Transform for TransformWindow {
    const NAME: &'static str = "TransformWindow";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        todo!()
    }

    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    fn on_finish(&mut self) -> Result<()> {
        todo!()
    }
}
