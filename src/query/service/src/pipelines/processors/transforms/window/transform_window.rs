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

    place: StateAddr,
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

    // used in Rank()
    current_row_number: u64,
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

    // advance the current row to the next row
    // if the current row is the last row of the current block, advance the current block and row = 0
    fn advance_row(&mut self, row: RowPtr) -> RowPtr {
        let mut row = row.clone();
        if row.row < self.block_rows(&row) - 1 {
            row.row += 1;
        } else {
            row.block += 1;
            row.row = 0;
        }
        row
    }

    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        if let Some(data) = data {
            self.blocks.push_back(WindowBlock {
                block: data,
                column_builder: todo!(),
            });
        }

        loop {
            self.advance_partition();

            while self.current_row < self.partition_end {
                self.advance_frame_start();
                if !self.frame_started {
                    break;
                }

                if self.frame_end < self.partition_end {
                    self.frame_end = self.partition_end;
                }

                self.advance_frame_end();
                if !self.frame_ended {
                    break;
                }

                self.apply_aggregate();
                self.result_current_row();

                self.prev_frame_start = self.frame_start;
                self.prev_frame_end = self.frame_end;

                self.current_row = self.advance_row(self.current_row);

                self.current_row_number += 1;
                self.frame_started = false;
                self.frame_ended = false;
            }

            if self.input_is_finished {
                return Ok(());
            }

            if !self.partition_ended {
                break;
            }

            // start to new partition
            self.partition_start = self.partition_end;
            self.current_row = self.advance_row(self.current_row);
            self.partition_ended = false;

            // reset frames
            self.frame_start = self.partition_start;
            self.frame_end = self.partition_start;
            self.prev_frame_start = self.partition_start;
            self.prev_frame_end = self.partition_start;

            self.current_row_number = 1;
            // reset states and builders
            todo!()
        }
        todo!()
    }

    fn apply_aggregate(&mut self) -> Result<()> {
        let mut reset_state = false;
        let mut row_start = self.frame_start;
        let row_end = self.frame_end;

        if self.frame_start == self.frame_end {
            row_start = self.prev_frame_end;
        } else {
            reset_state = true;
        }

        for block in row_start.block..=row_end.block {
            let block_start = if block == row_start.block {
                row_start.row
            } else {
                0
            };
            let block_end = if block == row_end.block {
                row_end.row
            } else {
                self.block_rows(&RowPtr { block, row: 0 })
            };

            for row in block_start..block_end {
                self.function.accumulate_row(self.place, &[], row)?;
            }
        }

        Ok(())
    }

    fn result_current_row(&mut self) {
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
        self.input_is_finished = true;
        self.add_block(None);

        todo!();
        Ok(())
    }
}
