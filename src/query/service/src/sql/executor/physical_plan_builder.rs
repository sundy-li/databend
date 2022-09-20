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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::catalog::CatalogManager;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::Expression;
use common_legacy_planners::Extras;
use common_legacy_planners::PrewhereInfo;
use common_legacy_planners::Projection;
use common_legacy_planners::StageKind;
use common_planner::IndexType;
use common_planner::Metadata;
use common_planner::MetadataRef;
use common_planner::DUMMY_TABLE_INDEX;
use common_storages_fuse::TableContext;
use itertools::Itertools;

use super::AggregateFinal;
use super::AggregatePartial;
use super::Exchange as PhysicalExchange;
use super::Filter;
use super::HashJoin;
use super::Limit;
use super::Project;
use super::Sort;
use super::TableScan;
use crate::catalogs::CatalogManagerHelper;
use crate::sessions::QueryContext;
use crate::sql::executor::util::check_physical;
use crate::sql::executor::AggregateFunctionDesc;
use crate::sql::executor::AggregateFunctionSignature;
use crate::sql::executor::ColumnID;
use crate::sql::executor::EvalScalar;
use crate::sql::executor::ExpressionBuilderWithoutRenaming;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalScalar;
use crate::sql::executor::SortDesc;
use crate::sql::executor::UnionAll;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregateMode;
use crate::sql::plans::Exchange;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::RelOperator;
use crate::sql::plans::Scalar;
use crate::sql::ScalarExpr;
use crate::storages::ToReadDataSourcePlan;

pub struct PhysicalPlanBuilder {
    metadata: MetadataRef,
    ctx: Arc<QueryContext>,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef, ctx: Arc<QueryContext>) -> Self {
        Self { metadata, ctx }
    }

    fn build_projection(
        metadata: &Metadata,
        schema: &DataSchemaRef,
        columns: &ColumnSet,
        has_inner_column: bool,
    ) -> Projection {
        if !has_inner_column {
            let col_indices = columns
                .iter()
                .map(|index| {
                    let name = metadata.column(*index).name();
                    schema.index_of(name).unwrap()
                })
                .sorted()
                .collect::<Vec<_>>();
            Projection::Columns(col_indices)
        } else {
            let col_indices = columns
                .iter()
                .map(|index| {
                    let column = metadata.column(*index);
                    match &column.path_indices() {
                        Some(path_indices) => (column.index(), path_indices.to_vec()),
                        None => {
                            let name = metadata.column(*index).name();
                            let idx = schema.index_of(name).unwrap();
                            (column.index(), vec![idx])
                        }
                    }
                })
                .sorted()
                .collect::<BTreeMap<_, Vec<IndexType>>>();
            Projection::InnerColumns(col_indices)
        }
    }

    #[async_recursion::async_recursion]
    pub async fn build(&self, s_expr: &SExpr) -> Result<PhysicalPlan> {
        debug_assert!(check_physical(s_expr));

        match s_expr.plan() {
            RelOperator::PhysicalScan(scan) => {
                let mut has_inner_column = false;
                let mut name_mapping = BTreeMap::new();
                let metadata = self.metadata.read().clone();
                for index in scan.columns.iter() {
                    let column = metadata.column(*index);
                    if column.has_path_indices() {
                        has_inner_column = true;
                    }
                    if let Some(prewhere) = &scan.prewhere {
                        // if there is a prewhere optimization,
                        // we can prune `PhysicalScan`'s ouput schema.
                        if prewhere.output_columns.contains(index) {
                            name_mapping.insert(column.name().to_string(), index.to_string());
                        }
                    } else {
                        let name = column.name().to_string();
                        name_mapping.insert(name, index.to_string());
                    }
                }

                let table_entry = metadata.table(scan.table_index);
                let table = table_entry.table();
                let table_schema = table.schema();

                let push_downs = self.push_downs(scan, &table_schema, has_inner_column)?;

                let source = table
                    .read_plan_with_catalog(
                        self.ctx.clone(),
                        table_entry.catalog().to_string(),
                        Some(push_downs),
                    )
                    .await?;
                Ok(PhysicalPlan::TableScan(TableScan {
                    name_mapping,
                    source: Box::new(source),
                    table_index: scan.table_index,
                }))
            }
            RelOperator::DummyTableScan(_) => {
                let catalogs = CatalogManager::instance();
                let table = catalogs
                    .get_catalog(CATALOG_DEFAULT)?
                    .get_table(self.ctx.get_tenant().as_str(), "system", "one")
                    .await?;
                let source = table
                    .read_plan_with_catalog(self.ctx.clone(), CATALOG_DEFAULT.to_string(), None)
                    .await?;
                Ok(PhysicalPlan::TableScan(TableScan {
                    name_mapping: BTreeMap::from([("dummy".to_string(), "dummy".to_string())]),
                    source: Box::new(source),
                    table_index: DUMMY_TABLE_INDEX,
                }))
            }
            RelOperator::PhysicalHashJoin(join) => {
                let build_side = self.build(s_expr.child(1)?).await?;
                let probe_side = self.build(s_expr.child(0)?).await?;
                Ok(PhysicalPlan::HashJoin(HashJoin {
                    build: Box::new(build_side),
                    probe: Box::new(probe_side),
                    join_type: join.join_type.clone(),
                    build_keys: join
                        .build_keys
                        .iter()
                        .map(|v| {
                            let mut builder = PhysicalScalarBuilder;
                            builder.build(v)
                        })
                        .collect::<Result<_>>()?,
                    probe_keys: join
                        .probe_keys
                        .iter()
                        .map(|v| {
                            let mut builder = PhysicalScalarBuilder;
                            builder.build(v)
                        })
                        .collect::<Result<_>>()?,
                    other_conditions: join
                        .other_conditions
                        .iter()
                        .map(|v| {
                            let mut builder = PhysicalScalarBuilder;
                            builder.build(v)
                        })
                        .collect::<Result<_>>()?,
                    marker_index: join.marker_index,
                    from_correlated_subquery: join.from_correlated_subquery,
                }))
            }
            RelOperator::Project(project) => {
                let input = self.build(s_expr.child(0)?).await?;
                let input_schema = input.output_schema()?;
                Ok(PhysicalPlan::Project(Project {
                    input: Box::new(input),
                    projections: project
                        .columns
                        .iter()
                        .sorted()
                        .map(|index| input_schema.index_of(index.to_string().as_str()))
                        .collect::<Result<_>>()?,

                    columns: project.columns.clone(),
                }))
            }
            RelOperator::EvalScalar(eval_scalar) => Ok(PhysicalPlan::EvalScalar(EvalScalar {
                input: Box::new(self.build(s_expr.child(0)?).await?),
                scalars: eval_scalar
                    .items
                    .iter()
                    .map(|item| {
                        let mut builder = PhysicalScalarBuilder;
                        Ok((builder.build(&item.scalar)?, item.index.to_string()))
                    })
                    .collect::<Result<_>>()?,
            })),

            RelOperator::Filter(filter) => Ok(PhysicalPlan::Filter(Filter {
                input: Box::new(self.build(s_expr.child(0)?).await?),
                predicates: filter
                    .predicates
                    .iter()
                    .map(|pred| {
                        let mut builder = PhysicalScalarBuilder;
                        builder.build(pred)
                    })
                    .collect::<Result<_>>()?,
            })),
            RelOperator::Aggregate(agg) => {
                let input = self.build(s_expr.child(0)?).await?;
                let group_items: Vec<ColumnID> = agg
                    .group_items
                    .iter()
                    .map(|v| v.index.to_string())
                    .collect();
                let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                    if let Scalar::AggregateFunction(agg) = &v.scalar {
                        Ok(AggregateFunctionDesc {
                            sig: AggregateFunctionSignature {
                                name: agg.func_name.clone(),
                                args: agg.args.iter().map(|s| {
                                    s.data_type()
                                }).collect(),
                                params: agg.params.clone(),
                                return_type: *agg.return_type.clone(),
                            },
                            column_id: v.index.to_string(),
                            args: agg.args.iter().map(|arg| {
                                if let Scalar::BoundColumnRef(col) = arg {
                                    Ok(col.column.index.to_string())
                                } else {
                                    Err(ErrorCode::LogicalError(
                                        "Aggregate function argument must be a BoundColumnRef".to_string()
                                    ))
                                }
                            }).collect::<Result<_>>()?,
                        })
                    } else {
                        Err(ErrorCode::LogicalError("Expected aggregate function".to_string()))
                    }
                }).collect::<Result<_>>()?;
                let result = match &agg.mode {
                    AggregateMode::Partial => match input {
                        PhysicalPlan::Exchange(PhysicalExchange { input, kind, .. }) => {
                            let aggregate_partial = AggregatePartial {
                                input,
                                agg_funcs,
                                group_by: group_items,
                            };

                            let output_schema = aggregate_partial.output_schema()?;
                            let group_by_key_field =
                                output_schema.field_with_name("_group_by_key")?;

                            PhysicalPlan::Exchange(PhysicalExchange {
                                kind,
                                input: Box::new(PhysicalPlan::AggregatePartial(aggregate_partial)),
                                keys: vec![PhysicalScalar::Variable {
                                    column_id: group_by_key_field.name().clone(),
                                    data_type: group_by_key_field.data_type().clone(),
                                }],
                            })
                        }
                        _ => PhysicalPlan::AggregatePartial(AggregatePartial {
                            agg_funcs,
                            group_by: group_items,
                            input: Box::new(input),
                        }),
                    },

                    // Hack to get before group by schema, we should refactor this
                    AggregateMode::Final => match input {
                        PhysicalPlan::AggregatePartial(ref agg) => {
                            let before_group_by_schema = agg.input.output_schema()?;
                            PhysicalPlan::AggregateFinal(AggregateFinal {
                                input: Box::new(input),
                                group_by: group_items,
                                agg_funcs,
                                before_group_by_schema,
                            })
                        }

                        PhysicalPlan::Exchange(PhysicalExchange {
                            input: box PhysicalPlan::AggregatePartial(ref agg),
                            ..
                        }) => {
                            let before_group_by_schema = agg.input.output_schema()?;
                            PhysicalPlan::AggregateFinal(AggregateFinal {
                                input: Box::new(input),
                                group_by: group_items,
                                agg_funcs,
                                before_group_by_schema,
                            })
                        }

                        _ => unreachable!(),
                    },
                    AggregateMode::Initial => unreachable!(),
                };

                Ok(result)
            }
            RelOperator::Sort(sort) => Ok(PhysicalPlan::Sort(Sort {
                input: Box::new(self.build(s_expr.child(0)?).await?),
                order_by: sort
                    .items
                    .iter()
                    .map(|v| SortDesc {
                        asc: v.asc,
                        nulls_first: v.nulls_first,
                        order_by: v.index.to_string(),
                    })
                    .collect(),
                limit: sort.limit,
            })),
            RelOperator::Limit(limit) => Ok(PhysicalPlan::Limit(Limit {
                input: Box::new(self.build(s_expr.child(0)?).await?),
                limit: limit.limit,
                offset: limit.offset,
            })),
            RelOperator::Exchange(exchange) => {
                let mut keys = vec![];
                let kind = match exchange {
                    Exchange::Hash(scalars) => {
                        for scalar in scalars {
                            let mut builder = PhysicalScalarBuilder;
                            keys.push(builder.build(scalar)?);
                        }
                        StageKind::Normal
                    }
                    Exchange::Broadcast => StageKind::Expansive,
                    Exchange::Merge => StageKind::Merge,
                };
                Ok(PhysicalPlan::Exchange(PhysicalExchange {
                    input: Box::new(self.build(s_expr.child(0)?).await?),
                    kind,
                    keys,
                }))
            }
            RelOperator::UnionAll(_) => {
                let left = self.build(s_expr.child(0)?).await?;
                let schema = left.output_schema()?;
                Ok(PhysicalPlan::UnionAll(UnionAll {
                    left: Box::new(left),
                    right: Box::new(self.build(s_expr.child(1)?).await?),
                    schema,
                }))
            }
            _ => Err(ErrorCode::LogicalError(format!(
                "Unsupported physical plan: {:?}",
                s_expr.plan()
            ))),
        }
    }

    fn push_downs(
        &self,
        scan: &PhysicalScan,
        table_schema: &DataSchemaRef,
        has_inner_column: bool,
    ) -> Result<Extras> {
        let metadata = self.metadata.read().clone();

        let projection =
            Self::build_projection(&metadata, table_schema, &scan.columns, has_inner_column);

        let push_down_filters = scan
            .push_down_predicates
            .clone()
            .map(|predicates| {
                let builder = ExpressionBuilderWithoutRenaming::create(self.metadata.clone());
                predicates
                    .into_iter()
                    .map(|scalar| builder.build(&scalar))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let prewhere_info = scan
            .prewhere
            .as_ref()
            .map(|prewhere| {
                let builder = ExpressionBuilderWithoutRenaming::create(self.metadata.clone());
                let predicates = prewhere
                    .predicates
                    .iter()
                    .map(|scalar| builder.build(scalar))
                    .collect::<Result<Vec<_>>>()?;

                assert!(
                    !predicates.is_empty(),
                    "There should be at least one predicate in prewhere"
                );
                let mut filter = predicates[0].clone();
                for pred in predicates.iter().skip(1) {
                    filter = filter.and(pred.clone());
                }

                let remain_columns = scan
                    .columns
                    .difference(&prewhere.prewhere_columns)
                    .copied()
                    .collect::<HashSet<usize>>();
                let output_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &prewhere.output_columns,
                    has_inner_column,
                );
                let prewhere_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &prewhere.prewhere_columns,
                    has_inner_column,
                );
                let remain_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &remain_columns,
                    has_inner_column,
                );

                Ok::<PrewhereInfo, ErrorCode>(PrewhereInfo {
                    output_columns,
                    prewhere_columns,
                    remain_columns,
                    filter,
                })
            })
            .transpose()?;

        let order_by = scan
            .order_by
            .clone()
            .map(|items| {
                let builder = ExpressionBuilderWithoutRenaming::create(self.metadata.clone());
                items
                    .into_iter()
                    .map(|item| {
                        builder
                            .build_column_ref(item.index)
                            .map(|c| Expression::Sort {
                                expr: Box::new(c.clone()),
                                asc: item.asc,
                                nulls_first: item.nulls_first,
                                origin_expr: Box::new(c),
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        Ok(Extras {
            projection: Some(projection),
            filters: push_down_filters.unwrap_or_default(),
            prewhere: prewhere_info,
            limit: scan.limit,
            order_by: order_by.unwrap_or_default(),
        })
    }
}

pub struct PhysicalScalarBuilder;

impl PhysicalScalarBuilder {
    pub fn build(&mut self, scalar: &Scalar) -> Result<PhysicalScalar> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => Ok(PhysicalScalar::Variable {
                column_id: column_ref.column.index.to_string(),
                data_type: column_ref.data_type(),
            }),
            Scalar::ConstantExpr(constant) => Ok(PhysicalScalar::Constant {
                value: constant.value.clone(),
                data_type: *constant.data_type.clone(),
            }),
            Scalar::AndExpr(and) => Ok(PhysicalScalar::Function {
                name: "and".to_string(),
                args: vec![
                    (self.build(&and.left)?, and.left.data_type()),
                    (self.build(&and.right)?, and.right.data_type()),
                ],
                return_type: and.data_type(),
            }),
            Scalar::OrExpr(or) => Ok(PhysicalScalar::Function {
                name: "or".to_string(),
                args: vec![
                    (self.build(&or.left)?, or.left.data_type()),
                    (self.build(&or.right)?, or.right.data_type()),
                ],
                return_type: or.data_type(),
            }),
            Scalar::ComparisonExpr(comp) => Ok(PhysicalScalar::Function {
                name: comp.op.to_func_name(),
                args: vec![
                    (self.build(&comp.left)?, comp.left.data_type()),
                    (self.build(&comp.right)?, comp.right.data_type()),
                ],
                return_type: comp.data_type(),
            }),
            Scalar::FunctionCall(func) => Ok(PhysicalScalar::Function {
                name: func.func_name.clone(),
                args: func
                    .arguments
                    .iter()
                    .zip(func.arg_types.iter())
                    .map(|(arg, typ)| Ok((self.build(arg)?, typ.clone())))
                    .collect::<Result<_>>()?,
                return_type: *func.return_type.clone(),
            }),
            Scalar::CastExpr(cast) => Ok(PhysicalScalar::Cast {
                input: Box::new(self.build(&cast.argument)?),
                target: *cast.target_type.clone(),
            }),

            _ => Err(ErrorCode::LogicalError(format!(
                "Unsupported physical scalar: {:?}",
                scalar
            ))),
        }
    }
}
