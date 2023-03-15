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

use std::collections::HashMap;

use async_recursion::async_recursion;
use common_ast::ast::Expr;
use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_ast::ast::OrderByExpr;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::SetOperator;
use common_ast::ast::TableReference;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::common_super_type;
use common_expression::types::DataType;
use common_functions::scalars::BUILTIN_FUNCTIONS;

use crate::binder::join::JoinConditions;
use crate::binder::scalar_common::split_conjunctions;
use crate::binder::CteInfo;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UnionAll;
use crate::ColumnBinding;
use crate::IndexType;

// A normalized IR for `SELECT` clause.
#[derive(Debug, Default)]
pub struct SelectList<'a> {
    pub items: Vec<SelectItem<'a>>,
}

#[derive(Debug)]
pub struct SelectItem<'a> {
    pub select_target: &'a SelectTarget,
    pub scalar: ScalarExpr,
    pub alias: String,
}

impl Binder {
    pub(super) async fn bind_select_stmt(
        &mut self,
        bind_context: &BindContext,
        stmt: &SelectStmt,
        order_by: &[OrderByExpr],
    ) -> Result<(SExpr, BindContext)> {
        let (mut s_expr, mut from_context) = if stmt.from.is_empty() {
            self.bind_one_table(bind_context, stmt).await?
        } else {
            let cross_joins = stmt
                .from
                .iter()
                .cloned()
                .reduce(|left, right| TableReference::Join {
                    span: None,
                    join: Join {
                        op: JoinOperator::CrossJoin,
                        condition: JoinCondition::None,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                })
                .unwrap();
            self.bind_table_reference(bind_context, &cross_joins)
                .await?
        };

        if let Some(expr) = &stmt.selection {
            s_expr = self.bind_where(&from_context, expr, s_expr).await?;
        }

        // Generate a analyzed select list with from context
        let mut select_list = self
            .normalize_select_list(&from_context, &stmt.select_list)
            .await?;

        let (mut scalar_items, projections) = self.analyze_projection(&select_list)?;

        // This will potentially add some alias group items to `from_context` if find some.
        self.analyze_group_items(&mut from_context, &select_list, &stmt.group_by)
            .await?;

        self.analyze_aggregate_select(&mut from_context, &mut select_list)?;

        let having = if let Some(having) = &stmt.having {
            Some(
                self.analyze_aggregate_having(&mut from_context, &select_list, having)
                    .await?,
            )
        } else {
            None
        };

        let order_items = self
            .analyze_order_items(
                &from_context,
                &mut scalar_items,
                &projections,
                order_by,
                stmt.distinct,
            )
            .await?;

        if !from_context.aggregate_info.aggregate_functions.is_empty()
            || !from_context.aggregate_info.group_items.is_empty()
        {
            s_expr = self.bind_aggregate(&mut from_context, s_expr).await?;
        }

        if let Some((having, span)) = having {
            s_expr = self
                .bind_having(&from_context, having, span, s_expr)
                .await?;
        }

        if stmt.distinct {
            s_expr = self.bind_distinct(&from_context, &projections, &mut scalar_items, s_expr)?;
        }

        if !order_by.is_empty() {
            s_expr = self
                .bind_order_by(
                    &from_context,
                    order_items,
                    &select_list,
                    &mut scalar_items,
                    s_expr,
                )
                .await?;
        }

        s_expr = self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;

        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context.columns = from_context.columns;
        output_context.ctes_map = from_context.ctes_map;

        Ok((s_expr, output_context))
    }

    #[async_recursion]
    pub(crate) async fn bind_set_expr(
        &mut self,
        bind_context: &BindContext,
        set_expr: &SetExpr,
        order_by: &[OrderByExpr],
    ) -> Result<(SExpr, BindContext)> {
        match set_expr {
            SetExpr::Select(stmt) => self.bind_select_stmt(bind_context, stmt, order_by).await,
            SetExpr::Query(stmt) => self.bind_query(bind_context, stmt).await,
            SetExpr::SetOperation(set_operation) => {
                self.bind_set_operator(
                    bind_context,
                    &set_operation.left,
                    &set_operation.right,
                    &set_operation.op,
                    &set_operation.all,
                )
                .await
            }
        }
    }

    #[async_recursion]
    pub(crate) async fn bind_query(
        &mut self,
        bind_context: &BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        if let Some(with) = &query.with {
            for cte in with.ctes.iter() {
                let table_name = cte.alias.name.name.clone();
                if bind_context.ctes_map.contains_key(&table_name) {
                    return Err(ErrorCode::SemanticError(format!(
                        "duplicate cte {table_name}"
                    )));
                }
                let cte_info = CteInfo {
                    columns_alias: cte.alias.columns.iter().map(|c| c.name.clone()).collect(),
                    query: cte.query.clone(),
                };
                bind_context.ctes_map.insert(table_name, cte_info);
            }
        }
        let (mut s_expr, bind_context) = match query.body {
            SetExpr::Select(_) | SetExpr::Query(_) => {
                self.bind_set_expr(bind_context, &query.body, &query.order_by)
                    .await?
            }
            SetExpr::SetOperation(_) => {
                let (mut s_expr, bind_context) =
                    self.bind_set_expr(bind_context, &query.body, &[]).await?;
                if !query.order_by.is_empty() {
                    s_expr = self
                        .bind_order_by_for_set_operation(&bind_context, s_expr, &query.order_by)
                        .await?;
                }
                (s_expr, bind_context)
            }
        };

        if !query.limit.is_empty() {
            if query.limit.len() == 1 {
                s_expr = self
                    .bind_limit(&bind_context, s_expr, Some(&query.limit[0]), &query.offset)
                    .await?;
            } else {
                s_expr = self
                    .bind_limit(
                        &bind_context,
                        s_expr,
                        Some(&query.limit[1]),
                        &Some(query.limit[0].clone()),
                    )
                    .await?;
            }
        } else if query.offset.is_some() {
            s_expr = self
                .bind_limit(&bind_context, s_expr, None, &query.offset)
                .await?;
        }

        Ok((s_expr, bind_context))
    }

    pub(super) async fn bind_where(
        &mut self,
        bind_context: &BindContext,
        expr: &Expr,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let (scalar, _) = scalar_binder.bind(expr).await?;
        let filter_plan = Filter {
            predicates: split_conjunctions(&scalar),
            is_having: false,
        };
        let new_expr = SExpr::create_unary(filter_plan.into(), child);
        Ok(new_expr)
    }

    pub(super) async fn bind_set_operator(
        &mut self,
        bind_context: &BindContext,
        left: &SetExpr,
        right: &SetExpr,
        op: &SetOperator,
        all: &bool,
    ) -> Result<(SExpr, BindContext)> {
        let (left_expr, left_bind_context) = self.bind_set_expr(bind_context, left, &[]).await?;
        let (right_expr, right_bind_context) = self.bind_set_expr(bind_context, right, &[]).await?;
        let mut coercion_types = Vec::with_capacity(left_bind_context.columns.len());
        if left_bind_context.columns.len() != right_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(
                "SetOperation must have the same number of columns",
            ));
        } else {
            for (left_col, right_col) in left_bind_context
                .columns
                .iter()
                .zip(right_bind_context.columns.iter())
            {
                if left_col.data_type != right_col.data_type {
                    let data_type = common_super_type(
                        *left_col.data_type.clone(),
                        *right_col.data_type.clone(),
                        &BUILTIN_FUNCTIONS.default_cast_rules,
                    )
                    .expect("SetOperation's types cannot be matched");
                    coercion_types.push(data_type);
                } else {
                    coercion_types.push(*left_col.data_type.clone());
                }
            }
        }
        match (op, all) {
            (SetOperator::Intersect, false) => {
                // Transfer Intersect to Semi join
                self.bind_intersect(left_bind_context, right_bind_context, left_expr, right_expr)
            }
            (SetOperator::Except, false) => {
                // Transfer Except to Anti join
                self.bind_except(left_bind_context, right_bind_context, left_expr, right_expr)
            }
            (SetOperator::Union, true) => self.bind_union(
                left_bind_context,
                right_bind_context,
                coercion_types,
                left_expr,
                right_expr,
                false,
            ),
            (SetOperator::Union, false) => self.bind_union(
                left_bind_context,
                right_bind_context,
                coercion_types,
                left_expr,
                right_expr,
                true,
            ),
            _ => Err(ErrorCode::Unimplemented(
                "Unsupported query type, currently, databend only support intersect distinct and except distinct",
            )),
        }
    }

    fn bind_union(
        &mut self,
        left_context: BindContext,
        right_context: BindContext,
        coercion_types: Vec<DataType>,
        left_expr: SExpr,
        right_expr: SExpr,
        distinct: bool,
    ) -> Result<(SExpr, BindContext)> {
        let (new_bind_context, pairs, left_expr, right_expr) = self.coercion_union_type(
            left_context,
            right_context,
            left_expr,
            right_expr,
            coercion_types,
        )?;

        let union_plan = UnionAll { pairs };
        let mut new_expr = SExpr::create_binary(union_plan.into(), left_expr, right_expr);
        if distinct {
            new_expr = self.bind_distinct(
                &new_bind_context,
                new_bind_context.all_column_bindings(),
                &mut HashMap::new(),
                new_expr,
            )?;
        }

        Ok((new_expr, new_bind_context))
    }

    fn bind_intersect(
        &mut self,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        self.bind_intersect_or_except(
            left_context,
            right_context,
            left_expr,
            right_expr,
            JoinType::LeftSemi,
        )
    }

    fn bind_except(
        &mut self,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        self.bind_intersect_or_except(
            left_context,
            right_context,
            left_expr,
            right_expr,
            JoinType::LeftAnti,
        )
    }

    fn bind_intersect_or_except(
        &mut self,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
        join_type: JoinType,
    ) -> Result<(SExpr, BindContext)> {
        let left_expr = self.bind_distinct(
            &left_context,
            left_context.all_column_bindings(),
            &mut HashMap::new(),
            left_expr,
        )?;
        let mut left_conditions = Vec::with_capacity(left_context.columns.len());
        let mut right_conditions = Vec::with_capacity(right_context.columns.len());
        assert_eq!(left_context.columns.len(), right_context.columns.len());
        for (left_column, right_column) in left_context
            .columns
            .iter()
            .zip(right_context.columns.iter())
        {
            left_conditions.push(
                BoundColumnRef {
                    column: left_column.clone(),
                }
                .into(),
            );
            right_conditions.push(
                BoundColumnRef {
                    column: right_column.clone(),
                }
                .into(),
            );
        }
        let join_conditions = JoinConditions {
            left_conditions,
            right_conditions,
            non_equi_conditions: vec![],
            other_conditions: vec![],
        };
        let s_expr = self.bind_join_with_type(join_type, join_conditions, left_expr, right_expr)?;
        Ok((s_expr, left_context))
    }

    #[allow(clippy::type_complexity)]
    fn coercion_union_type(
        &self,
        left_bind_context: BindContext,
        right_bind_context: BindContext,
        mut left_expr: SExpr,
        mut right_expr: SExpr,
        coercion_types: Vec<DataType>,
    ) -> Result<(BindContext, Vec<(IndexType, IndexType)>, SExpr, SExpr)> {
        let mut left_scalar_items = Vec::with_capacity(left_bind_context.columns.len());
        let mut right_scalar_items = Vec::with_capacity(right_bind_context.columns.len());
        let mut new_bind_context = BindContext::new();
        let mut pairs = Vec::with_capacity(left_bind_context.columns.len());
        for (idx, (left_col, right_col)) in left_bind_context
            .columns
            .iter()
            .zip(right_bind_context.columns.iter())
            .enumerate()
        {
            let left_index = if *left_col.data_type != coercion_types[idx] {
                let new_column_index = self
                    .metadata
                    .write()
                    .add_derived_column(left_col.column_name.clone(), coercion_types[idx].clone());
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    column_name: left_col.column_name.clone(),
                    index: new_column_index,
                    data_type: Box::new(coercion_types[idx].clone()),
                    visibility: Visibility::Visible,
                };
                let left_coercion_expr = CastExpr {
                    is_try: false,
                    argument: Box::new(
                        BoundColumnRef {
                            column: left_col.clone(),
                        }
                        .into(),
                    ),
                    target_type: Box::new(coercion_types[idx].clone()),
                };
                left_scalar_items.push(ScalarItem {
                    scalar: left_coercion_expr.into(),
                    index: new_column_index,
                });
                new_bind_context.add_column_binding(column_binding);
                new_column_index
            } else {
                new_bind_context.add_column_binding(left_col.clone());
                left_col.index
            };
            let right_index = if *right_col.data_type != coercion_types[idx] {
                let new_column_index = self
                    .metadata
                    .write()
                    .add_derived_column(right_col.column_name.clone(), coercion_types[idx].clone());
                let right_coercion_expr = CastExpr {
                    is_try: false,
                    argument: Box::new(
                        BoundColumnRef {
                            column: right_col.clone(),
                        }
                        .into(),
                    ),
                    target_type: Box::new(coercion_types[idx].clone()),
                };
                right_scalar_items.push(ScalarItem {
                    scalar: right_coercion_expr.into(),
                    index: new_column_index,
                });
                new_column_index
            } else {
                right_col.index
            };
            pairs.push((left_index, right_index));
        }
        if !left_scalar_items.is_empty() {
            left_expr = SExpr::create_unary(
                EvalScalar {
                    items: left_scalar_items,
                }
                .into(),
                left_expr,
            );
        }
        if !right_scalar_items.is_empty() {
            right_expr = SExpr::create_unary(
                EvalScalar {
                    items: right_scalar_items,
                }
                .into(),
                right_expr,
            );
        }
        Ok((new_bind_context, pairs, left_expr, right_expr))
    }
}
