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

use common_ast::ast::AlterTableAction;
use common_ast::ast::AlterTableStmt;
use common_ast::ast::AnalyzeTableStmt;
use common_ast::ast::CompactTarget;
use common_ast::ast::CreateTableSource;
use common_ast::ast::CreateTableStmt;
use common_ast::ast::DescribeTableStmt;
use common_ast::ast::DropTableStmt;
use common_ast::ast::Engine;
use common_ast::ast::ExistsTableStmt;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::OptimizeTableAction as AstOptimizeTableAction;
use common_ast::ast::OptimizeTableStmt;
use common_ast::ast::RenameTableStmt;
use common_ast::ast::ShowCreateTableStmt;
use common_ast::ast::ShowLimit;
use common_ast::ast::ShowTablesStatusStmt;
use common_ast::ast::ShowTablesStmt;
use common_ast::ast::Statement;
use common_ast::ast::TableReference;
use common_ast::ast::TruncateTableStmt;
use common_ast::ast::UndropTableStmt;
use common_ast::ast::UriLocation;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::walk_expr_mut;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_datavalues::type_coercion::compare_coercion;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::ToDataType;
use common_datavalues::TypeFactory;
use common_datavalues::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::DataOperator;
use common_storages_table_meta::table::is_reserved_opt_key;
use common_storages_table_meta::table::OPT_KEY_DATABASE_ID;
use common_storages_view::view_table::QUERY;
use common_storages_view::view_table::VIEW_ENGINE;
use tracing::debug;

use crate::binder::location::parse_uri_location;
use crate::binder::scalar::ScalarBinder;
use crate::binder::Binder;
use crate::binder::Visibility;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerConfig;
use crate::optimizer::OptimizerContext;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::IdentifierNormalizer;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CastExpr;
use crate::plans::CreateTablePlanV2;
use crate::plans::DescribeTablePlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTablePlan;
use crate::plans::ExistsTablePlan;
use crate::plans::OptimizeTableAction;
use crate::plans::OptimizeTablePlan;
use crate::plans::Plan;
use crate::plans::ReclusterTablePlan;
use crate::plans::RenameTableEntity;
use crate::plans::RenameTablePlan;
use crate::plans::RevertTablePlan;
use crate::plans::RewriteKind;
use crate::plans::Scalar;
use crate::plans::ShowCreateTablePlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UndropTablePlan;
use crate::BindContext;
use crate::ColumnBinding;
use crate::Planner;
use crate::ScalarExpr;
use crate::SelectBuilder;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn bind_show_tables(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowTablesStmt<'a>,
    ) -> Result<Plan> {
        let ShowTablesStmt {
            catalog,
            database,
            full,
            limit,
            with_history,
        } = stmt;

        let database = self.check_database_exist(database).await?;

        let mut select_builder = if stmt.with_history {
            SelectBuilder::from("system.tables_with_history")
        } else {
            SelectBuilder::from("system.tables")
        };

        if *full {
            select_builder
                .with_column("name AS Tables")
                .with_column("'BASE TABLE' AS Table_type")
                .with_column("database AS Database")
                .with_column("catalog AS Catalog")
                .with_column("engine")
                .with_column("created_on AS create_time");
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            }

            select_builder
                .with_column("num_rows")
                .with_column("data_size")
                .with_column("data_compressed_size")
                .with_column("index_size");
        } else {
            select_builder.with_column(format!("name AS Tables_in_{database}"));
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            };
        }

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));

        if let Some(catalog) = catalog {
            let catalog = normalize_identifier(catalog, &self.name_resolution_ctx).name;
            select_builder.with_filter(format!("catalog = '{catalog}'"));
        }

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show tables rewrite to: {:?}", query);
        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowTables)
            .await
    }

    pub(in crate::planner::binder) async fn bind_show_create_table(
        &mut self,
        stmt: &ShowCreateTableStmt<'a>,
    ) -> Result<Plan> {
        let ShowCreateTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", Vu8::to_data_type()),
            DataField::new("Create Table", Vu8::to_data_type()),
        ]);
        Ok(Plan::ShowCreateTable(Box::new(ShowCreateTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    pub(in crate::planner::binder) async fn bind_describe_table(
        &mut self,
        stmt: &DescribeTableStmt<'a>,
    ) -> Result<Plan> {
        let DescribeTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Field", Vu8::to_data_type()),
            DataField::new("Type", Vu8::to_data_type()),
            DataField::new("Null", Vu8::to_data_type()),
            DataField::new("Default", Vu8::to_data_type()),
            DataField::new("Extra", Vu8::to_data_type()),
        ]);

        Ok(Plan::DescribeTable(Box::new(DescribeTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    pub(in crate::planner::binder) async fn bind_show_tables_status(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowTablesStatusStmt<'a>,
    ) -> Result<Plan> {
        let ShowTablesStatusStmt { database, limit } = stmt;

        let database = self.check_database_exist(database).await?;

        let select_cols = "name AS Name, engine AS Engine, 0 AS Version, \
        NULL AS Row_format, num_rows AS Rows, NULL AS Avg_row_length, data_size AS Data_length, \
        NULL AS Max_data_length, index_size AS Index_length, NULL AS Data_free, NULL AS Auto_increment, \
        created_on AS Create_time, NULL AS Update_time, NULL AS Check_time, NULL AS Collation, \
        NULL AS Checksum, '' AS Comment"
            .to_string();

        // Use `system.tables` AS the "base" table to construct the result-set of `SHOW TABLE STATUS ..`
        //
        // To constraint the schema of the final result-set,
        //  `(select ${select_cols} from system.tables where ..)`
        // is used AS a derived table.
        // (unlike mysql, alias of derived table is not required in databend).
        let query = match limit {
            None => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
                ORDER BY Name",
                select_cols, database
            ),
            Some(ShowLimit::Like { pattern }) => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
            WHERE Name LIKE '{}' ORDER BY Name",
                select_cols, database, pattern
            ),
            Some(ShowLimit::Where { selection }) => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
            WHERE ({}) ORDER BY Name",
                select_cols, database, selection
            ),
        };
        let tokens = tokenize_sql(query.as_str())?;
        let backtrace = Backtrace::new();
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
        self.bind_statement(bind_context, &stmt).await
    }

    async fn check_database_exist(&mut self, database: &Option<Identifier<'_>>) -> Result<String> {
        match database {
            None => Ok(self.ctx.get_current_database()),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx
                    .get_catalog(&self.ctx.get_current_catalog())?
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                Ok(database)
            }
        }
    }

    pub(in crate::planner::binder) async fn bind_create_table(
        &mut self,
        stmt: &CreateTableStmt<'a>,
    ) -> Result<Plan> {
        let CreateTableStmt {
            if_not_exists,
            catalog,
            database,
            table,
            source,
            table_options,
            cluster_by,
            as_query,
            transient,
            engine,
            uri_location,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        // Take FUSE engine AS default engine
        let engine = engine.unwrap_or(Engine::Fuse);
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        for table_option in table_options.iter() {
            self.insert_table_option_with_validation(
                &mut options,
                table_option.0.to_lowercase(),
                table_option.1.to_string(),
            )?;
        }

        let storage_params = match uri_location {
            Some(uri) => {
                let uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    connection: uri.connection.clone(),
                };
                let (sp, _) = parse_uri_location(&uri)?;

                // create a temporary op to check if params is correct
                DataOperator::try_create(&sp).await?;

                Some(sp)
            }
            None => None,
        };

        // If table is TRANSIENT, set a flag in table option
        if *transient {
            options.insert("TRANSIENT".to_owned(), "T".to_owned());
        }

        // Build table schema
        let (schema, field_default_exprs, field_comments) = match (&source, &as_query) {
            (Some(source), None) => {
                // `CREATE TABLE` without `AS SELECT ...`
                self.analyze_create_table_schema(source).await?
            }
            (None, Some(query)) => {
                // `CREATE TABLE AS SELECT ...` without column definitions
                let init_bind_context = BindContext::new();
                let (_s_expr, bind_context) = self.bind_query(&init_bind_context, query).await?;
                let fields = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        DataField::new(
                            &column_binding.column_name,
                            *column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                let schema = DataSchemaRefExt::create(fields);
                Self::validate_create_table_schema(&schema)?;
                (schema, vec![], vec![])
            }
            (Some(source), Some(query)) => {
                // e.g. `CREATE TABLE t (i INT) AS SELECT * from old_t` with columns speicified
                let (source_schema, source_default_exprs, source_coments) =
                    self.analyze_create_table_schema(source).await?;
                let init_bind_context = BindContext::new();
                let (_s_expr, bind_context) = self.bind_query(&init_bind_context, query).await?;
                let query_fields: Vec<DataField> = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        DataField::new(
                            &column_binding.column_name,
                            *column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                let source_fields = source_schema.fields().clone();
                let source_fields = self.concat_fields(source_fields, query_fields);
                let schema = DataSchemaRefExt::create(source_fields);
                Self::validate_create_table_schema(&schema)?;
                (schema, source_default_exprs, source_coments)
            }
            _ => Err(ErrorCode::BadArguments(
                "Incorrect CREATE query: required list of column descriptions or AS section or SELECT..",
            ))?,
        };

        if engine == Engine::Fuse {
            // Currently, [Table] can not accesses its database id yet, thus
            // here we keep the db id AS an entry of `table_meta.options`.
            //
            // To make the unit/stateless test cases (`show create ..`) easier,
            // here we care about the FUSE engine only.
            //
            // Later, when database id is kept, let say in `TableInfo`, we can
            // safely eliminate this "FUSE" constant and the table meta option entry.
            let catalog = self.ctx.get_catalog(&catalog)?;
            let db = catalog
                .get_database(&self.ctx.get_tenant(), &database)
                .await?;
            let db_id = db.get_db_info().ident.db_id;
            options.insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
        }

        let cluster_key = {
            let keys = self
                .analyze_cluster_keys(cluster_by, schema.clone())
                .await?;
            if keys.is_empty() {
                None
            } else {
                Some(format!("({})", keys.join(", ")))
            }
        };

        let plan = CreateTablePlanV2 {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            catalog,
            database,
            table,
            schema,
            engine,
            storage_params,
            options,
            field_default_exprs,
            field_comments,
            cluster_key,
            as_select: if let Some(query) = as_query {
                let bind_context = BindContext::new();
                let stmt = Statement::Query(Box::new(*query.clone()));
                let select_plan = self.bind_statement(&bind_context, &stmt).await?;
                // Don't enable distributed optimization for `CREATE TABLE ... AS SELECT ...` for now
                let opt_ctx = Arc::new(OptimizerContext::new(OptimizerConfig::default()));
                let optimized_plan = optimize(self.ctx.clone(), opt_ctx, select_plan)?;
                Some(Box::new(optimized_plan))
            } else {
                None
            },
        };
        Ok(Plan::CreateTable(Box::new(plan)))
    }

    pub(in crate::planner::binder) async fn bind_drop_table(
        &mut self,
        stmt: &DropTableStmt<'a>,
    ) -> Result<Plan> {
        let DropTableStmt {
            if_exists,
            catalog,
            database,
            table,
            all,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::DropTable(Box::new(DropTablePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            table,
            all: *all,
        })))
    }

    pub(in crate::planner::binder) async fn bind_undrop_table(
        &mut self,
        stmt: &UndropTableStmt<'a>,
    ) -> Result<Plan> {
        let UndropTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::UndropTable(Box::new(UndropTablePlan {
            tenant,
            catalog,
            database,
            table,
        })))
    }

    pub(in crate::planner::binder) async fn bind_alter_table(
        &mut self,
        bind_context: &BindContext,
        stmt: &AlterTableStmt<'a>,
    ) -> Result<Plan> {
        let AlterTableStmt {
            if_exists,
            table_reference,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let (catalog, database, table) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table_reference
        {
            (
                catalog.as_ref().map_or_else(
                    || self.ctx.get_current_catalog(),
                    |i| normalize_identifier(i, &self.name_resolution_ctx).name,
                ),
                database.as_ref().map_or_else(
                    || self.ctx.get_current_database(),
                    |i| normalize_identifier(i, &self.name_resolution_ctx).name,
                ),
                normalize_identifier(table, &self.name_resolution_ctx).name,
            )
        } else {
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

        match action {
            AlterTableAction::RenameTable { new_table } => {
                let entities = vec![RenameTableEntity {
                    if_exists: *if_exists,
                    new_database: database.clone(),
                    new_table: normalize_identifier(new_table, &self.name_resolution_ctx).name,
                    catalog,
                    database,
                    table,
                }];

                Ok(Plan::RenameTable(Box::new(RenameTablePlan {
                    tenant,
                    entities,
                })))
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                let schema = self
                    .ctx
                    .get_table(&catalog, &database, &table)
                    .await?
                    .schema();
                let cluster_keys = self.analyze_cluster_keys(cluster_by, schema).await?;

                Ok(Plan::AlterTableClusterKey(Box::new(
                    AlterTableClusterKeyPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        cluster_keys,
                    },
                )))
            }
            AlterTableAction::DropTableClusterKey => Ok(Plan::DropTableClusterKey(Box::new(
                DropTableClusterKeyPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                },
            ))),
            AlterTableAction::ReclusterTable {
                is_final,
                selection,
            } => {
                let (_, context) = self
                    .bind_table_reference(bind_context, table_reference)
                    .await?;

                let mut scalar_binder = ScalarBinder::new(
                    &context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );

                let push_downs = if let Some(expr) = selection {
                    let (scalar, _) = scalar_binder.bind(expr).await?;
                    Some(scalar)
                } else {
                    None
                };

                Ok(Plan::ReclusterTable(Box::new(ReclusterTablePlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    is_final: *is_final,
                    metadata: self.metadata.clone(),
                    push_downs,
                })))
            }
            AlterTableAction::RevertTo { point } => {
                let point = self.resolve_data_travel_point(bind_context, point).await?;
                Ok(Plan::RevertTable(Box::new(RevertTablePlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    point,
                })))
            }
        }
    }

    pub(in crate::planner::binder) async fn bind_rename_table(
        &mut self,
        stmt: &RenameTableStmt<'a>,
    ) -> Result<Plan> {
        let RenameTableStmt {
            if_exists,
            catalog,
            database,
            table,
            new_catalog,
            new_database,
            new_table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;
        let new_catalog = new_catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let new_database = new_database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let new_table = normalize_identifier(new_table, &self.name_resolution_ctx).name;

        if new_catalog != catalog {
            return Err(ErrorCode::BadArguments(
                "alter catalog not allowed while reanme table",
            ));
        }

        let entities = vec![RenameTableEntity {
            if_exists: *if_exists,
            catalog,
            database,
            table,
            new_database,
            new_table,
        }];

        Ok(Plan::RenameTable(Box::new(RenameTablePlan {
            tenant,
            entities,
        })))
    }

    pub(in crate::planner::binder) async fn bind_truncate_table(
        &mut self,
        stmt: &TruncateTableStmt<'a>,
    ) -> Result<Plan> {
        let TruncateTableStmt {
            catalog,
            database,
            table,
            purge,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::TruncateTable(Box::new(TruncateTablePlan {
            catalog,
            database,
            table,
            purge: *purge,
        })))
    }

    pub(in crate::planner::binder) async fn bind_optimize_table(
        &mut self,
        stmt: &OptimizeTableStmt<'a>,
    ) -> Result<Plan> {
        let OptimizeTableStmt {
            catalog,
            database,
            table,
            action,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;
        let action = if let Some(ast_action) = action {
            match ast_action {
                AstOptimizeTableAction::All => OptimizeTableAction::All,
                AstOptimizeTableAction::Purge => OptimizeTableAction::Purge,
                AstOptimizeTableAction::Compact { target, limit } => {
                    let limit_cnt = match limit {
                        Some(Expr::Literal {
                            lit: Literal::Integer(uint),
                            ..
                        }) => Some(*uint as usize),
                        Some(_) => {
                            return Err(ErrorCode::IllegalDataType("Unsupported limit type"));
                        }
                        _ => None,
                    };
                    match target {
                        CompactTarget::Block => OptimizeTableAction::CompactBlocks(limit_cnt),
                        CompactTarget::Segment => OptimizeTableAction::CompactSegments(limit_cnt),
                    }
                }
            }
        } else {
            OptimizeTableAction::Purge
        };

        Ok(Plan::OptimizeTable(Box::new(OptimizeTablePlan {
            catalog,
            database,
            table,
            action,
        })))
    }

    pub(in crate::planner::binder) async fn bind_analyze_table(
        &mut self,
        stmt: &AnalyzeTableStmt<'a>,
    ) -> Result<Plan> {
        let AnalyzeTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::AnalyzeTable(Box::new(AnalyzeTablePlan {
            catalog,
            database,
            table,
        })))
    }

    pub(in crate::planner::binder) async fn bind_exists_table(
        &mut self,
        stmt: &ExistsTableStmt<'a>,
    ) -> Result<Plan> {
        let ExistsTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table = normalize_identifier(table, &self.name_resolution_ctx).name;

        Ok(Plan::ExistsTable(Box::new(ExistsTablePlan {
            catalog,
            database,
            table,
        })))
    }

    async fn analyze_create_table_schema(
        &self,
        source: &CreateTableSource<'a>,
    ) -> Result<(DataSchemaRef, Vec<Option<Scalar>>, Vec<String>)> {
        let bind_context = BindContext::new();
        match source {
            CreateTableSource::Columns(columns) => {
                let mut scalar_binder = ScalarBinder::new(
                    &bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                let mut fields = Vec::with_capacity(columns.len());
                let mut fields_default_expr = Vec::with_capacity(columns.len());
                let mut fields_comments = Vec::with_capacity(columns.len());
                for column in columns.iter() {
                    let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
                    let data_type = TypeFactory::instance().get(column.data_type.to_string())?;

                    fields.push(DataField::new(&name, data_type.clone()));
                    fields_default_expr.push({
                        if let Some(default_expr) = &column.default_expr {
                            let (mut expr, expr_type) = scalar_binder.bind(default_expr).await?;
                            if compare_coercion(&data_type, &expr_type).is_err() {
                                return Err(ErrorCode::SemanticError(format!("column {name} is of type {} but default expression is of type {}", data_type, expr_type)));
                            }
                            if !expr_type.eq(&data_type) {
                                expr = Scalar::CastExpr(CastExpr {
                                    argument: Box::new(expr),
                                    from_type: Box::new(expr_type),
                                    target_type: Box::new(data_type),
                                })
                            }
                            Some(expr)
                        } else {
                            None
                        }
                    });
                    fields_comments.push(column.comment.clone().unwrap_or_default());
                }
                let schema = DataSchemaRefExt::create(fields);
                Self::validate_create_table_schema(&schema)?;
                Ok((schema, fields_default_expr, fields_comments))
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => {
                let catalog = catalog
                    .as_ref()
                    .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database = database.as_ref().map_or_else(
                    || self.ctx.get_current_database(),
                    |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
                );
                let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
                let table = self.ctx.get_table(&catalog, &database, &table_name).await?;

                if table.engine() == VIEW_ENGINE {
                    let query = table.get_table_info().options().get(QUERY).unwrap();
                    let mut planner = Planner::new(self.ctx.clone());
                    let (plan, _, _) = planner.plan_sql(query).await?;
                    Ok((plan.schema(), vec![], vec![]))
                } else {
                    Ok((table.schema(), vec![], table.field_comments().clone()))
                }
            }
        }
    }

    /// Validate the schema of the table to be created.
    fn validate_create_table_schema(schema: &DataSchemaRef) -> Result<()> {
        // Check if there are duplicated column names
        let mut name_set = HashSet::new();
        for field in schema.fields() {
            if !name_set.insert(field.name().clone()) {
                return Err(ErrorCode::BadArguments(format!(
                    "Duplicated column name: {}",
                    field.name()
                )));
            }
        }

        Ok(())
    }

    fn insert_table_option_with_validation(
        &self,
        options: &mut BTreeMap<String, String>,
        key: String,
        value: String,
    ) -> Result<()> {
        if is_reserved_opt_key(&key) {
            Err(ErrorCode::TableOptionInvalid(format!(
                "table option {key} reserved, please do not specify in the CREATE TABLE statement",
            )))
        } else if options.insert(key.clone(), value).is_some() {
            Err(ErrorCode::TableOptionInvalid(format!(
                "table option {key} duplicated"
            )))
        } else {
            Ok(())
        }
    }

    async fn analyze_cluster_keys(
        &mut self,
        cluster_by: &[Expr<'a>],
        schema: DataSchemaRef,
    ) -> Result<Vec<String>> {
        // Build a temporary BindContext to resolve the expr
        let mut bind_context = BindContext::new();
        for field in schema.fields() {
            let column = ColumnBinding {
                database_name: None,
                table_name: None,
                column_name: field.name().clone(),
                // A dummy index is fine, since we won't actually evaluate the expression
                index: 0,
                data_type: Box::new(field.data_type().clone()),
                visibility: Visibility::Visible,
            };
            bind_context.columns.push(column);
        }
        let mut scalar_binder = ScalarBinder::new(
            &bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );

        let mut cluster_keys = Vec::with_capacity(cluster_by.len());
        for cluster_by in cluster_by.iter() {
            let (cluster_key, _) = scalar_binder.bind(cluster_by).await?;
            if !cluster_key.is_deterministic() {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is not deterministic",
                    cluster_by
                )));
            }
            let mut cluster_by = cluster_by.clone();
            walk_expr_mut(
                &mut IdentifierNormalizer {
                    ctx: &self.name_resolution_ctx,
                },
                &mut cluster_by,
            );
            cluster_keys.push(format!("{:#}", &cluster_by));
        }

        Ok(cluster_keys)
    }

    fn concat_fields(
        &self,
        mut source_fields: Vec<DataField>,
        query_fields: Vec<DataField>,
    ) -> Vec<DataField> {
        let mut name_set = HashSet::new();
        for field in source_fields.iter() {
            name_set.insert(field.name().clone());
        }
        for query_field in query_fields.iter() {
            if !name_set.contains(query_field.name()) {
                source_fields.push(query_field.clone());
            }
        }
        source_fields
    }
}
