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

use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::assert_string;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct ConcatFunction {
    _display_name: String,
}

impl ConcatFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        for arg in args {
            assert_string(*arg)?;
        }
        Ok(Box::new(ConcatFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 1024),
        )
    }
}

impl Function for ConcatFunction {
    fn name(&self) -> &str {
        "concat"
    }

    fn return_type(&self) -> DataTypePtr {
        Vu8::to_data_type()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let viewers = columns
            .iter()
            .map(|c| Vu8::try_create_viewer(c.column()))
            .collect::<Result<Vec<_>>>()?;

        let mut values: Vec<u8> = Vec::with_capacity(input_rows * columns.len());
        let mut offsets: Vec<i64> = Vec::with_capacity(input_rows + 1);
        offsets.push(0);

        for row in 0..input_rows {
            for viewer in viewers.iter() {
                values.extend_from_slice(viewer.value_at(row));
            }
            offsets.push(values.len() as i64);
        }

        let mut builder = MutableStringColumn::from_data(values, offsets);
        Ok(builder.to_column())
    }
}

impl fmt::Display for ConcatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CONCAT")
    }
}
