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
use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value as JsonValue;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct CheckJsonFunction {
    display_name: String,
}

impl CheckJsonFunction {
    pub fn try_create(display_name: &str, _args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        Ok(Box::new(CheckJsonFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for CheckJsonFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypePtr {
        NullableType::arc(StringType::arc())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let data_type = columns[0].field().data_type();
        let column = columns[0].column();

        let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(input_rows);

        if data_type.data_type_id().is_numeric() || data_type.data_type_id() == TypeID::Boolean {
            for _i in 0..input_rows {
                builder.append_null()
            }
        } else if data_type.data_type_id() == TypeID::String {
            let c: &StringColumn = Series::check_get(column)?;
            for v in c.iter() {
                match std::str::from_utf8(v) {
                    Ok(v) => match serde_json::from_str::<JsonValue>(v) {
                        Ok(_v) => builder.append_null(),
                        Err(e) => builder.append(e.to_string().as_bytes(), true),
                    },
                    Err(e) => builder.append(e.to_string().as_bytes(), true),
                }
            }
        } else if data_type.data_type_id() == TypeID::Variant {
            let c: &ObjectColumn<JsonValue> = Series::check_get(column)?;
            for v in c.iter() {
                if let JsonValue::String(s) = v {
                    match serde_json::from_str::<JsonValue>(s.as_str()) {
                        Ok(_v) => builder.append_null(),
                        Err(e) => builder.append(e.to_string().as_bytes(), true),
                    }
                } else {
                    builder.append_null()
                }
            }
        } else if data_type.data_type_id().is_date_or_date_time() {
            for _i in 0..input_rows {
                builder.append(
                    format!("{:?} is not a valid JSON", data_type.data_type_id()).as_bytes(),
                    true,
                )
            }
        } else {
            return Err(ErrorCode::BadDataValueType(format!(
                "Invalid argument types for function 'CHECK_JSON': {:?}",
                column.data_type_id()
            )));
        }

        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for CheckJsonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
