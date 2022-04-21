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

use std::sync::Arc;

use common_exception::Result;
use common_planners::PlanNode;
use common_planners::RenameDatabaseEntity;
use common_planners::RenameDatabasePlan;
use common_tracing::tracing;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::resolve_database;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfAlterDatabase {
    pub if_exists: bool,
    pub database_name: ObjectName,
    pub action: AlterDatabaseAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AlterDatabaseAction {
    RenameDatabase(ObjectName),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfAlterDatabase {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let tenant = ctx.get_tenant();
        let (catalog, db) = resolve_database(&ctx, &self.database_name, "ALTER DATABASE")?;

        match &self.action {
            AlterDatabaseAction::RenameDatabase(o) => {
                let mut entities = Vec::new();
                let (_new_catalog, new_db) = resolve_database(&ctx, o, "ALTER DATABASE")?;
                entities.push(RenameDatabaseEntity {
                    if_exists: self.if_exists,
                    catalog_name: catalog,
                    db,
                    new_db,
                });

                Ok(AnalyzedResult::SimpleQuery(Box::new(
                    PlanNode::RenameDatabase(RenameDatabasePlan { tenant, entities }),
                )))
            }
        }
    }
}
