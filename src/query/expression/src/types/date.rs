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

use std::fmt::Display;
use std::io::Cursor;
use std::ops::Range;

use chrono::NaiveDate;
use chrono_tz::Tz;
use common_arrow::arrow::buffer::Buffer;
use common_datavalues::DateConverter;
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::ReadBytesExt;
use num_traits::AsPrimitive;

use super::number::SimpleDomain;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub const DATE_FORMAT: &str = "%Y-%m-%d";
/// Minimum valid date, represented by the day offset from 1970-01-01.
pub const DATE_MIN: i32 = -354285;
/// Maximum valid date, represented by the day offset from 1970-01-01.
pub const DATE_MAX: i32 = 2932896;

/// Check if date is within range.
#[inline]
pub fn check_date(days: i64) -> Result<i32, String> {
    if (DATE_MIN as i64..=DATE_MAX as i64).contains(&days) {
        Ok(days as i32)
    } else {
        Err(format!(
            "date `{}` is out of range",
            date_to_string(days, chrono_tz::Tz::UTC)
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateType;

impl ValueType for DateType {
    type Scalar = i32;
    type ScalarRef<'a> = i32;
    type Column = Buffer<i32>;
    type Domain = SimpleDomain<i32>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, i32>>;
    type ColumnBuilder = Vec<i32>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: i32) -> i32 {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Date(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::Date(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<SimpleDomain<i32>> {
        domain.as_date().map(SimpleDomain::clone)
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Date(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Date(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Date(col)
    }

    fn upcast_domain(domain: SimpleDomain<i32>) -> Domain {
        Domain::Date(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index).cloned()
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        *col.get_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().slice(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter().cloned()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        buffer_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::Scalar) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Self::Scalar::default());
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.extend_from_slice(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }
}

impl ArgType for DateType {
    fn data_type() -> DataType {
        DataType::Date
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: DATE_MIN,
            max: DATE_MAX,
        }
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.into()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

#[inline]
pub fn string_to_date(date_str: impl AsRef<[u8]>, tz: Tz) -> Option<NaiveDate> {
    let mut reader = Cursor::new(std::str::from_utf8(date_str.as_ref()).unwrap().as_bytes());
    match reader.read_date_text(&tz) {
        Ok(d) => match reader.must_eof() {
            Ok(..) => Some(d),
            Err(_) => None,
        },
        Err(_) => None,
    }
}

#[inline]
pub fn date_to_string(date: impl AsPrimitive<i64>, tz: Tz) -> impl Display {
    date.as_().to_date(&tz).format(DATE_FORMAT)
}
