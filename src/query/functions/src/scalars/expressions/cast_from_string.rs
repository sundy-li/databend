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

use std::io::Cursor;

use chrono::DateTime;
use chrono::Datelike;
use chrono::NaiveDate;
use chrono_tz::Tz;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::ReadBytesExt;

use super::cast_with_type::arrow_cast_compute;
use super::cast_with_type::CastOptions;
use crate::scalars::FunctionContext;

pub fn cast_from_string(
    column: &ColumnRef,
    from_type: &DataTypeImpl,
    data_type: &DataTypeImpl,
    cast_options: &CastOptions,
    func_ctx: &FunctionContext,
) -> Result<(ColumnRef, Option<Bitmap>)> {
    let str_column = Series::remove_nullable(column);
    let str_column: &StringColumn = Series::check_get(&str_column)?;
    let size = str_column.len();

    let return_res = |column| {
        let nullable_column: &NullableColumn = Series::check_get(&column)?;
        Ok((
            nullable_column.inner().clone(),
            Some(nullable_column.ensure_validity().clone()),
        ))
    };
    match data_type.data_type_id() {
        TypeID::Date => {
            let mut builder = NullableColumnBuilder::<i32>::with_capacity(size);
            let tz = func_ctx.tz;
            for v in str_column.iter() {
                match string_to_date(v, &tz) {
                    Some(d) => builder.append(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE, true),
                    None => builder.append_null(),
                }
            }
            let column = builder.build(size);
            return_res(column)
        }
        TypeID::Timestamp => {
            let mut builder = NullableColumnBuilder::<i64>::with_capacity(size);
            let tz = func_ctx.tz;
            for v in str_column.iter() {
                match string_to_timestamp(v, &tz) {
                    Some(d) => builder.append(d.timestamp_micros(), true),
                    None => builder.append_null(),
                }
            }
            let column = builder.build(size);
            return_res(column)
        }
        TypeID::Boolean => {
            let mut builder = NullableColumnBuilder::<bool>::with_capacity(size);
            for v in str_column.iter() {
                if v.eq_ignore_ascii_case("true".as_bytes()) {
                    builder.append(true, true);
                } else if v.eq_ignore_ascii_case("false".as_bytes()) {
                    builder.append(false, true);
                } else {
                    builder.append_null();
                }
            }
            let column = builder.build(size);
            return_res(column)
        }
        TypeID::Interval => todo!(),
        _ => arrow_cast_compute(column, from_type, data_type, cast_options, func_ctx),
    }
}

// TODO support timezone
#[inline]
pub fn string_to_timestamp(date_str: impl AsRef<[u8]>, tz: &Tz) -> Option<DateTime<Tz>> {
    let mut reader = Cursor::new(std::str::from_utf8(date_str.as_ref()).unwrap().as_bytes());
    match reader.read_timestamp_text(tz) {
        Ok(dt) => match reader.must_eof() {
            Ok(..) => Some(dt),
            Err(_) => None,
        },
        Err(_) => None,
    }
}

#[inline]
pub fn string_to_date(date_str: impl AsRef<[u8]>, tz: &Tz) -> Option<NaiveDate> {
    let mut reader = Cursor::new(std::str::from_utf8(date_str.as_ref()).unwrap().as_bytes());
    match reader.read_date_text(tz) {
        Ok(d) => match reader.must_eof() {
            Ok(..) => Some(d),
            Err(_) => None,
        },
        Err(_) => None,
    }
}
