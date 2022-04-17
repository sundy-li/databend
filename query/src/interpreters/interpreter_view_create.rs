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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::GrantObject;
use common_meta_types::TableMeta;
use common_meta_types::UserPrivilegeType;
use common_planners::CreateViewPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::storages::view::view_table::VIEW_ENGINE;

pub struct CreateViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateViewPlan,
}

impl CreateViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateViewPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateViewInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateViewInterpreter {
    fn name(&self) -> &str {
        "CreateViewInterpreter"
    }

    async fn execute(
        &self,
        _: Option<SendableDataBlockStream>,
        _source_pipe_builder: Option<SourcePipeBuilder>,
    ) -> Result<SendableDataBlockStream> {
        // check privilige
        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Database(self.plan.db.clone()),
                UserPrivilegeType::Create,
            )
            .await?;

        // check whether view has exists
        if self
            .ctx
            .get_catalog()
            .list_tables(&*self.plan.tenant, &*self.plan.db)
            .await?
            .iter()
            .any(|table| table.name() == self.plan.viewname.as_str())
        {
            return Err(ErrorCode::ViewAlreadyExists(format!(
                "{}.{} as view Already Exists",
                self.plan.db, self.plan.viewname
            )));
        }

        self.create_view().await
    }
}

impl CreateViewInterpreter {
    async fn create_view(&self) -> Result<SendableDataBlockStream> {
        let catalog = self.ctx.get_catalog();
        let mut options = BTreeMap::new();
        options.insert("query".to_string(), self.plan.subquery.clone());
        let plan = CreateTableReq {
            if_not_exists: self.plan.if_not_exists,
            tenant: self.plan.tenant.clone(),
            db_name: self.plan.db.clone(),
            table_name: self.plan.viewname.clone(),
            table_meta: TableMeta {
                engine: VIEW_ENGINE.to_string(),
                options,
                ..Default::default()
            },
        };
        catalog.create_table(plan).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
