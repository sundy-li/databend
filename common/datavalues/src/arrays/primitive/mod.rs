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

mod builder;
mod iterator;
mod mutable;

pub use builder::*;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::arity::unary;
use common_arrow::arrow::compute::cast;
use common_arrow::arrow::compute::cast::CastOptions;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;
pub use iterator::*;
pub use mutable::*;

use crate::prelude::*;

/// DFPrimitiveArray is generic struct which wrapped arrow's PrimitiveArray
#[derive(Debug, Clone)]
pub struct DFPrimitiveArray<T: DFPrimitiveType> {
    pub(crate) array: PrimitiveArray<T>,
}

impl<T: DFPrimitiveType> From<PrimitiveArray<T>> for DFPrimitiveArray<T> {
    fn from(array: PrimitiveArray<T>) -> Self {
        Self::new(array)
    }
}

fn precision(x: &TimeUnit) -> usize {
    match x {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

impl<T: DFPrimitiveType> DFPrimitiveArray<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        Self { array }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let expected_type = create_primitive_datatype::<T>();
        let expected_arrow = expected_type.arrow_type();
        let cast_options = CastOptions {
            wrapped: true,
            partial: true,
        };

        if &expected_arrow != array.data_type() {
            match array.data_type() {
                // u32
                ArrowDataType::Timestamp(x, _) => {
                    let p = precision(x);
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .expect("primitive cast should be ok");

                    let array = unary(array, |x| (x as usize / p) as u32, expected_arrow);

                    Self::from_arrow_array(&array)
                }
                ArrowDataType::Date32 => {
                    let array = cast::cast(array, &ArrowDataType::Int32, cast_options)
                        .expect("primitive cast should be ok");
                    let array = cast::cast(array.as_ref(), &expected_arrow, cast_options)
                        .expect("primitive cast should be ok");

                    Self::from_arrow_array(array.as_ref())
                }
                ArrowDataType::Date64 => {
                    let array = cast::cast(array, &ArrowDataType::Int64, cast_options)
                        .expect("primitive cast should be ok");
                    let array = cast::cast(array.as_ref(), &expected_arrow, cast_options)
                        .expect("primitive cast should be ok");

                    Self::from_arrow_array(array.as_ref())
                }
                ArrowDataType::Time32(x) => {
                    let p = precision(x);
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .expect("primitive cast should be ok");

                    let array = unary(array, |x| (x as usize / p) as u32, expected_arrow);

                    Self::from_arrow_array(&array)
                }
                ArrowDataType::Time64(x) => {
                    let p = precision(x);
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .expect("primitive cast should be ok");

                    let array = unary(array, |x| (x as usize / p) as u32, expected_arrow);

                    Self::from_arrow_array(&array)
                }
                _ => unreachable!(),
            }
        } else {
            let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            Self::new(array.clone())
        }
    }

    pub fn data_type(&self) -> DataTypePtr {
        create_primitive_datatype::<T>()
    }

    pub fn inner(&self) -> &PrimitiveArray<T> {
        &self.array
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = self.array.value_unchecked(index);

        if self.array.is_null(index) {
            return Ok(DataValue::Null);
        }

        // it's safe to unwrap here, because we know the type is correct
        match (T::SIGN, T::FLOATING) {
            (false, false) => Ok(DataValue::UInt64(v.to_u64().unwrap())),
            (true, false) => Ok(DataValue::Int64(v.to_i64().unwrap())),
            (_, true) => Ok(DataValue::Float64(v.to_f64().unwrap())),
        }
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type().type_id() == array.type_id() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.array.validity()
    }

    /// Take a view of top n elements
    #[must_use]
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Create a new DataArray by taking ownership of the Vec. This operation is zero copy.
    pub fn new_from_vec(values: Vec<T>) -> Self {
        to_primitive::<T>(values, None)
    }

    /// Nullify values in slice with an existing null bitmap
    pub fn new_from_owned_with_null_bitmap(values: Vec<T>, validity: Option<Bitmap>) -> Self {
        to_primitive::<T>(values, validity)
    }

    pub fn collect_values(&self) -> Vec<Option<T>> {
        let e = self.array.iter().map(|c| c.copied());
        e.collect()
    }
}

#[inline]
pub fn to_primitive<T: DFPrimitiveType>(
    values: Vec<T>,
    validity: Option<Bitmap>,
) -> DFPrimitiveArray<T> {
    PrimitiveArray::from_data(T::data_type().to_arrow(), values.into(), validity).into()
}

pub type DFUInt8Array = DFPrimitiveArray<u8>;
pub type DFUInt16Array = DFPrimitiveArray<u16>;
pub type DFUInt32Array = DFPrimitiveArray<u32>;
pub type DFUInt64Array = DFPrimitiveArray<u64>;

pub type DFInt8Array = DFPrimitiveArray<i8>;
pub type DFInt16Array = DFPrimitiveArray<i16>;
pub type DFInt32Array = DFPrimitiveArray<i32>;
pub type DFInt64Array = DFPrimitiveArray<i64>;

pub type DFFloat32Array = DFPrimitiveArray<f32>;
pub type DFFloat64Array = DFPrimitiveArray<f64>;

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for single chunk without nulls and an iterator as index.
pub unsafe fn take_primitive_iter_unchecked<T: DFPrimitiveType, I: IntoIterator<Item = usize>>(
    arr: &PrimitiveArray<T>,
    indices: I,
) -> PrimitiveArray<T> {
    match arr.null_count() {
        0 => {
            let array_values = arr.values().as_slice();
            let iter = indices
                .into_iter()
                .map(|idx| *array_values.get_unchecked(idx));

            let values = Buffer::from_trusted_len_iter_unchecked(iter);
            let data_type = create_primitive_datatype::<T>();
            PrimitiveArray::from_data(data_type.arrow_type(), values, None)
        }
        _ => {
            let array_values = arr.values();

            let iter = indices.into_iter().map(|idx| {
                if arr.is_valid(idx) {
                    Some(array_values[idx])
                } else {
                    None
                }
            });
            PrimitiveArray::from_trusted_len_iter_unchecked(iter).to(T::data_type().to_arrow())
        }
    }
}

/// # Safety
/// Note this doesn't do any bound checking, for performance reason.
/// Take kernel for a single chunk and an iterator that can produce None values.
/// This is used in join operations.
pub unsafe fn take_primitive_opt_iter_unchecked<
    T: DFPrimitiveType,
    I: IntoIterator<Item = Option<usize>>,
>(
    arr: &PrimitiveArray<T>,
    indices: I,
) -> PrimitiveArray<T> {
    match arr.null_count() {
        0 => {
            let array_values = arr.values();

            let iter = indices
                .into_iter()
                .map(|opt_idx| opt_idx.map(|idx| *array_values.get_unchecked(idx)));
            PrimitiveArray::from_trusted_len_iter_unchecked(iter)
        }
        _ => {
            let array_values = arr.values();

            let iter = indices.into_iter().map(|opt_idx| {
                opt_idx.and_then(|idx| {
                    if arr.is_valid(idx) {
                        Some(*array_values.get_unchecked(idx))
                    } else {
                        None
                    }
                })
            });

            PrimitiveArray::from_trusted_len_iter_unchecked(iter).to(T::data_type().to_arrow())
        }
    }
}
