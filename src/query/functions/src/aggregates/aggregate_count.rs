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

use std::alloc::Layout;
use std::fmt;
use std::sync::Arc;

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_variadic_arguments;

pub struct AggregateCountState {
    count: u64,
}

#[derive(Clone)]
pub struct AggregateCountFunction {
    display_name: String,
    nullable: bool,
}

impl AggregateCountFunction {
    pub fn try_create(
        display_name: &str,
        _params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_variadic_arguments(display_name, arguments.len(), (0, 1))?;
        Ok(Arc::new(AggregateCountFunction {
            display_name: display_name.to_string(),
            nullable: false,
        }))
    }

    pub fn desc() -> AggregateFunctionDescription {
        let features = super::aggregate_function_factory::AggregateFunctionFeatures {
            returns_default_when_only_null: true,
            ..Default::default()
        };
        AggregateFunctionDescription::creator_with_features(Box::new(Self::try_create), features)
    }

    pub fn name() -> &'static str {
        "AggregateCountFunction"
    }
}

impl AggregateFunction for AggregateCountFunction {
    fn name(&self) -> &str {
        Self::name()
    }

    fn return_type(&self) -> Result<DataTypeImpl> {
        Ok(u64::to_data_type())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateCountState { count: 0 });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateCountState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateCountState>();

        if self.nullable {
            let (_, bm) = columns[0].validity();
            let nulls = match combine_validities(bm, validity) {
                Some(b) => b.unset_bits(),
                None => 0,
            };
            state.count += (input_rows - nulls) as u64;
        } else {
            let nulls = match validity {
                Some(b) => b.unset_bits(),
                None => 0,
            };
            state.count += (input_rows - nulls) as u64;
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let validity = match columns.len() {
            0 => None,
            _ => {
                let (_, validity) = columns[0].validity();
                validity
            }
        };

        match validity {
            Some(v) => {
                for (valid, place) in v.iter().zip(places.iter()) {
                    if valid {
                        let state = place.next(offset).get::<AggregateCountState>();
                        state.count += 1;
                    }
                }
            }
            None => {
                for place in places {
                    let state = place.next(offset).get::<AggregateCountState>();
                    state.count += 1;
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, _columns: &[ColumnRef], _row: usize) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        state.count += 1;
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        serialize_into_buf(writer, &state.count)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        state.count = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateCountState>();
        let rhs = rhs.get::<AggregateCountState>();
        state.count += rhs.count;

        Ok(())
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let builder: &mut MutablePrimitiveColumn<u64> = Series::check_get_mutable_column(array)?;
        let state = place.get::<AggregateCountState>();
        builder.append_value(state.count);
        Ok(())
    }

    fn get_own_null_adaptor(
        &self,
        _nested_function: super::AggregateFunctionRef,
        _params: Vec<DataValue>,
        _arguments: Vec<DataField>,
    ) -> Result<Option<super::AggregateFunctionRef>> {
        let mut f = self.clone();
        f.nullable = true;
        Ok(Some(Arc::new(f)))
    }
}

impl fmt::Display for AggregateCountFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
