// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Formatter;

use common_datavalues::DataSchemaRef;
use common_meta_types::UserStageInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct S3StageTableInfo {
    pub schema: DataSchemaRef,
    pub stage_info: UserStageInfo,
    pub path: String,
    pub files: Vec<String>,
}

impl S3StageTableInfo {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn desc(&self) -> String {
        self.stage_info.stage_name.clone()
    }
}

impl Debug for S3StageTableInfo {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.stage_info)
    }
}
