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
mod transform_window;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd)]
pub(crate) struct RowPtr {
    pub block: usize,
    pub row: usize,
}

impl RowPtr {
    pub fn new(block: usize, row: usize) -> Self {
        Self { block, row }
    }
}
