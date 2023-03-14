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
use std::ops::Range;

use common_exception::Result;
use common_expression::DataBlock;
use common_functions::aggregates::AggregateFunction;
use common_functions::aggregates::StateAddr;
use common_pipeline_transforms::processors::transforms::Transform;

use super::RowPtr;

pub struct TransformWindow {
    function: Arc<dyn AggregateFunction>,
    state: StateAddr,

    partition_indices: Vec<usize>,
    order_by_indices: Vec<usize>,

    arena: bumpalo::Bump,
    /// A queue of data blocks that we need to process.
    /// If partition is ended, we may free the data block from front of the queue.
    blocks: VecDeque<DataBlock>,

    /// monotonically increasing index of the current block in the queue.
    current_block_index: usize,

    partition_range: Range<RowPtr>,
    partition_end: bool,

    frame_range: Range<RowPtr>,
    frame_started: bool,
    frame_ended: bool,

    current_row: RowPtr,
}

impl TransformWindow {
    fn create() -> Self {
        todo!()
    }

    /// Advance the partition end to the next partition or the end of the data.
    fn advance_partition_end(&mut self) {
        todo!()
    }

    fn advance_frame_start(&mut self) {
        if self.frame_start {
            return;
        }
        todo!()
    }

    fn advance_frame_end(&mut self) {
        todo!()
    }
}

impl Transform for TransformWindow {
    const NAME: &'static str = "TransformWindow";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.blocks.push_back(data);
        todo!()
    }

    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    fn on_finish(&mut self) -> Result<()> {
        todo!()
    }
}
