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

use std::hash::Hash;

use common_ast::ast::Query;
use common_ast::ast::TableAlias;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use dashmap::DashMap;

use super::AggregateInfo;
use crate::normalize_identifier;
use crate::plans::ScalarExpr;
use crate::IndexType;
use crate::NameResolutionContext;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Visibility {
    // Default for a column
    Visible,
    // Inner column of struct
    InVisible,
    // Consider the sql: `select * from t join t1 using(a)`.
    // The result should only contain one `a` column.
    // So we need make `t.a` or `t1.a` invisible in unqualified
    UnqualifiedWildcardInVisible,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ColumnBinding {
    /// Database name of this `ColumnBinding` in current context
    pub database_name: Option<String>,
    /// Table name of this `ColumnBinding` in current context
    pub table_name: Option<String>,
    /// Column name of this `ColumnBinding` in current context
    pub column_name: String,
    /// Column index of ColumnBinding
    pub index: IndexType,

    pub data_type: Box<DataType>,

    pub visibility: Visibility,
}

impl PartialEq for ColumnBinding {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for ColumnBinding {}

impl Hash for ColumnBinding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

#[derive(Debug, Clone)]
pub enum NameResolutionResult {
    Column(ColumnBinding),
    Alias { alias: String, scalar: ScalarExpr },
}

/// `BindContext` stores all the free variables in a query and tracks the context of binding procedure.
#[derive(Clone, Debug)]
pub struct BindContext {
    pub parent: Option<Box<BindContext>>,

    pub columns: Vec<ColumnBinding>,

    pub aggregate_info: AggregateInfo,

    /// True if there is aggregation in current context, which means
    /// non-grouping columns cannot be referenced outside aggregation
    /// functions, otherwise a grouping error will be raised.
    pub in_grouping: bool,

    pub ctes_map: Box<DashMap<String, CteInfo>>,

    pub is_view: bool,
}

#[derive(Clone, Debug)]
pub struct CteInfo {
    pub columns_alias: Vec<String>,
    pub query: Query,
}

impl BindContext {
    pub fn new() -> Self {
        Self {
            parent: None,
            columns: Vec::new(),
            aggregate_info: AggregateInfo::default(),
            in_grouping: false,
            ctes_map: Box::new(DashMap::new()),
            is_view: false,
        }
    }

    pub fn with_parent(parent: Box<BindContext>) -> Self {
        BindContext {
            parent: Some(parent.clone()),
            columns: vec![],
            aggregate_info: Default::default(),
            in_grouping: false,
            ctes_map: parent.ctes_map.clone(),
            is_view: false,
        }
    }

    /// Create a new BindContext with self's parent as its parent
    pub fn replace(&self) -> Self {
        let mut bind_context = BindContext::new();
        bind_context.parent = self.parent.clone();
        bind_context.ctes_map = self.ctes_map.clone();
        bind_context
    }

    /// Generate a new BindContext and take current BindContext as its parent.
    pub fn push(self) -> Self {
        Self::with_parent(Box::new(self))
    }

    /// Returns all column bindings in current scope.
    pub fn all_column_bindings(&self) -> &[ColumnBinding] {
        &self.columns
    }

    pub fn add_column_binding(&mut self, column_binding: ColumnBinding) {
        self.columns.push(column_binding);
    }

    /// Apply table alias like `SELECT * FROM t AS t1(a, b, c)`.
    /// This method will rename column bindings according to table alias.
    pub fn apply_table_alias(
        &mut self,
        alias: &TableAlias,
        name_resolution_ctx: &NameResolutionContext,
    ) -> Result<()> {
        for column in self.columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(normalize_identifier(&alias.name, name_resolution_ctx).name);
        }

        if alias.columns.len() > self.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "table has {} columns available but {} columns specified",
                self.columns.len(),
                alias.columns.len()
            )));
        }
        for (index, column_name) in alias
            .columns
            .iter()
            .map(|ident| normalize_identifier(ident, name_resolution_ctx).name)
            .enumerate()
        {
            self.columns[index].column_name = column_name;
        }
        Ok(())
    }

    /// Try to find a column binding with given table name and column name.
    /// This method will return error if the given names are ambiguous or invalid.
    pub fn resolve_name(
        &self,
        database: Option<&str>,
        table: Option<&str>,
        column: &str,
        span: Span,
        available_aliases: &[(String, ScalarExpr)],
    ) -> Result<NameResolutionResult> {
        let mut result = vec![];

        let mut bind_context: &BindContext = self;
        // Lookup parent context to resolve outer reference.
        loop {
            // TODO(leiysky): use `Identifier` for alias instead of raw string
            for (alias, scalar) in available_aliases {
                if database.is_none() && table.is_none() && column == alias {
                    result.push(NameResolutionResult::Alias {
                        alias: alias.clone(),
                        scalar: scalar.clone(),
                    });
                }
            }

            // We will lookup alias first. If there are matched aliases, we will skip
            // looking up `BindContext` to avoid ambiguity.
            if !result.is_empty() {
                break;
            }

            for column_binding in bind_context.columns.iter() {
                if Self::match_column_binding(database, table, column, column_binding) {
                    result.push(NameResolutionResult::Column(column_binding.clone()));
                }
            }
            if !result.is_empty() {
                break;
            }

            if let Some(ref parent) = bind_context.parent {
                bind_context = parent;
            } else {
                break;
            }
        }

        if result.is_empty() {
            Err(ErrorCode::SemanticError(format!("column {column} doesn't exist")).set_span(span))
        } else if result.len() > 1 {
            Err(ErrorCode::SemanticError(format!(
                "column {column} reference is ambiguous, got {result:?}"
            ))
            .set_span(span))
        } else {
            Ok(result.remove(0))
        }
    }

    pub fn match_column_binding(
        database: Option<&str>,
        table: Option<&str>,
        column: &str,
        column_binding: &ColumnBinding,
    ) -> bool {
        match (
            (database, column_binding.database_name.as_ref()),
            (table, column_binding.table_name.as_ref()),
        ) {
            // No qualified table name specified
            ((None, _), (None, None)) | ((None, _), (None, Some(_)))
                if column == column_binding.column_name =>
            {
                column_binding.visibility != Visibility::UnqualifiedWildcardInVisible
            }

            // Qualified column reference without database name
            ((None, _), (Some(table), Some(table_name)))
                if table == table_name && column == column_binding.column_name =>
            {
                true
            }

            // Qualified column reference with database name
            ((Some(db), Some(db_name)), (Some(table), Some(table_name)))
                if db == db_name && table == table_name && column == column_binding.column_name =>
            {
                true
            }
            _ => false,
        }
    }

    /// Get result columns of current context in order.
    /// For example, a query `SELECT b, a AS b FROM t` has `[(index_of(b), "b"), index_of(a), "b"]` as
    /// its result columns.
    ///
    /// This method is used to retrieve the physical representation of result set of
    /// a query.
    pub fn result_columns(&self) -> Vec<(IndexType, String)> {
        self.columns
            .iter()
            .map(|col| (col.index, col.column_name.clone()))
            .collect()
    }

    /// Return data scheme.
    pub fn output_schema(&self) -> DataSchemaRef {
        let fields = self
            .columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.column_name,
                    *column_binding.data_type.clone(),
                )
            })
            .collect();
        DataSchemaRefExt::create(fields)
    }
}

impl Default for BindContext {
    fn default() -> Self {
        BindContext::new()
    }
}
