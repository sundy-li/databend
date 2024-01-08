// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::is_internal_column;
use databend_common_expression::is_stream_column;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::types::decimal::Decimal256Type;
use databend_common_expression::types::decimal::DecimalDataType;
use databend_common_expression::types::decimal::DecimalDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::ColumnId;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::Index;

#[derive(Clone)]
pub struct RangeIndex {
    expr: Expr<String>,
    func_ctx: FunctionContext,
    schema: TableSchemaRef,

    // Default stats for each column if no stats are available (e.g. for new-add columns)
    default_stats: StatisticsOfColumns,
}

impl RangeIndex {
    pub fn try_create(
        func_ctx: FunctionContext,
        expr: &Expr<String>,
        schema: TableSchemaRef,
        default_stats: StatisticsOfColumns,
    ) -> Result<Self> {
        Ok(Self {
            expr: expr.clone(),
            func_ctx,
            schema,
            default_stats,
        })
    }

    pub fn try_apply_const(&self) -> Result<bool> {
        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(self.expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }

    #[minitrace::trace]
    pub fn apply<F>(
        &self,
        stats: &StatisticsOfColumns,
        location: Option<(String, u64)>,
        column_is_default: F,
    ) -> Result<bool>
    where
        F: Fn(&ColumnId) -> bool,
    {
        let input_domains = self
            .expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                // internal column and stream column are not actual stored columns
                // variant type may be virtual columns that are not included in leaf columns
                if is_internal_column(&name)
                    || is_stream_column(&name)
                    || ty.remove_nullable() == DataType::Variant
                {
                    return Ok((name, Domain::full(&ty)));
                }

                let column_ids = self.schema.leaf_columns_of(&name);
                assert!(
                    !column_ids.is_empty(),
                    "column {} not found in schema {:?}",
                    name,
                    &self.schema
                );

                let stats: Vec<&ColumnStatistics> = column_ids
                    .iter()
                    .filter_map(|column_id| match stats.get(column_id) {
                        None => {
                            if column_is_default(column_id)
                                && self.default_stats.contains_key(column_id)
                            {
                                Some(&self.default_stats[column_id])
                            } else {
                                None
                            }
                        }
                        other => other,
                    })
                    .collect();

                // DEBUG LOGS
                if ty.remove_nullable().is_string() && !stats.is_empty() {
                    let stat = &stats[0];

                    let mismatch_nullable =
                        !ty.is_nullable_or_null() && (stat.max().is_null() || stat.min().is_null());
                    let mismatch_string = ty.is_string()
                        && (stat.max().as_string().is_none() || stat.min().as_string().is_none());

                    let mismatch_nullable_none_string = ty.is_nullable()
                        && !stat.max.is_null()
                        && !stat.min.is_null()
                        && (stat.max().as_string().is_none() || stat.min().as_string().is_none());

                    if mismatch_nullable || mismatch_string || mismatch_nullable_none_string {
                        log::error!(
                            "StringDomain is incorrect, location: {:?}, type: {:?}, doamin: {:?}",
                            location,
                            ty,
                            stat
                        );
                    }
                }

                let domain = statistics_to_domain(stats, &ty);
                Ok((name, domain))
            })
            .collect::<Result<_>>()?;

        let (new_expr, _) = ConstantFolder::fold_with_domain(
            &self.expr,
            &input_domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(new_expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }

    #[minitrace::trace]
    pub fn apply_with_partition_columns(
        &self,
        stats: &StatisticsOfColumns,
        partition_columns: &HashMap<String, Scalar>,
    ) -> Result<bool> {
        let expr = self.expr.fill_const_column(partition_columns);
        RangeIndex {
            expr,
            func_ctx: self.func_ctx.clone(),
            schema: self.schema.clone(),
            default_stats: self.default_stats.clone(),
        }
        .apply(stats, None, |_| false)
    }
}

pub fn statistics_to_domain(mut stats: Vec<&ColumnStatistics>, data_type: &DataType) -> Domain {
    if stats.len() != data_type.num_leaf_columns() {
        return Domain::full(data_type);
    }
    match data_type {
        DataType::Nullable(box inner_ty) => {
            if stats.len() == 1 && (stats[0].min().is_null() || stats[0].max().is_null()) {
                return Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                });
            }
            let has_null = if stats.len() == 1 {
                stats[0].null_count > 0
            } else {
                // Only leaf columns have statistics,
                // nested columns are treated as having nullable values
                true
            };
            let domain = statistics_to_domain(stats, inner_ty);
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(Box::new(domain)),
            })
        }
        DataType::Tuple(inner_tys) => {
            let inner_domains = inner_tys
                .iter()
                .map(|inner_ty| {
                    let n = inner_ty.num_leaf_columns();
                    let stats = stats.drain(..n).collect();
                    statistics_to_domain(stats, inner_ty)
                })
                .collect::<Vec<_>>();
            Domain::Tuple(inner_domains)
        }
        DataType::Array(box inner_ty) => {
            let n = inner_ty.num_leaf_columns();
            let stats = stats.drain(..n).collect();
            let inner_domain = statistics_to_domain(stats, inner_ty);
            Domain::Array(Some(Box::new(inner_domain)))
        }
        DataType::Map(box inner_ty) => {
            let n = inner_ty.num_leaf_columns();
            let stats = stats.drain(..n).collect();
            let inner_domain = statistics_to_domain(stats, inner_ty);
            let kv_domain = inner_domain.as_tuple().unwrap();
            Domain::Map(Some((
                Box::new(kv_domain[0].clone()),
                Box::new(kv_domain[1].clone()),
            )))
        }
        _ => {
            let stat = stats[0];
            with_number_mapped_type!(|NUM_TYPE| match data_type {
                DataType::Number(NumberDataType::NUM_TYPE) => {
                    NumberType::<NUM_TYPE>::upcast_domain(SimpleDomain {
                        min: NumberType::<NUM_TYPE>::try_downcast_scalar(&stat.min().as_ref())
                            .unwrap(),
                        max: NumberType::<NUM_TYPE>::try_downcast_scalar(&stat.max().as_ref())
                            .unwrap(),
                    })
                }
                DataType::String => {
                    if stat.min().as_string().is_none() || stat.max().as_string().is_none() {
                        log::error!("StringDomain is incorrect, {:?}", stat);
                    }
                    Domain::String(StringDomain {
                        min: StringType::try_downcast_scalar(&stat.min().as_ref())
                            .unwrap()
                            .to_vec(),
                        max: Some(
                            StringType::try_downcast_scalar(&stat.max().as_ref())
                                .unwrap()
                                .to_vec(),
                        ),
                    })
                }
                DataType::Timestamp => TimestampType::upcast_domain(SimpleDomain {
                    min: TimestampType::try_downcast_scalar(&stat.min().as_ref()).unwrap(),
                    max: TimestampType::try_downcast_scalar(&stat.max().as_ref()).unwrap(),
                }),
                DataType::Date => DateType::upcast_domain(SimpleDomain {
                    min: DateType::try_downcast_scalar(&stat.min().as_ref()).unwrap(),
                    max: DateType::try_downcast_scalar(&stat.max().as_ref()).unwrap(),
                }),
                DataType::Decimal(dec) => match dec {
                    DecimalDataType::Decimal128(sz) => Domain::Decimal(DecimalDomain::Decimal128(
                        SimpleDomain {
                            min: Decimal128Type::try_downcast_scalar(&stat.min().as_ref()).unwrap(),
                            max: Decimal128Type::try_downcast_scalar(&stat.max().as_ref()).unwrap(),
                        },
                        *sz,
                    )),
                    DecimalDataType::Decimal256(sz) => Domain::Decimal(DecimalDomain::Decimal256(
                        SimpleDomain {
                            min: Decimal256Type::try_downcast_scalar(&stat.min().as_ref()).unwrap(),
                            max: Decimal256Type::try_downcast_scalar(&stat.max().as_ref()).unwrap(),
                        },
                        *sz,
                    )),
                },
                // Unsupported data type
                _ => Domain::full(data_type),
            })
        }
    }
}

impl Index for RangeIndex {}
