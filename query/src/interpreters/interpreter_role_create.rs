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

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_meta_types::RoleInfo;
use common_planners::CreateRolePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreateRoleInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateRolePlan,
}

impl CreateRoleInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateRolePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateRoleInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateRoleInterpreter {
    fn name(&self) -> &str {
        "CreateRoleInterpreter"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        // TODO: add privilege check about CREATE ROLE
        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        let role_info = RoleInfo::new(plan.role_name);
        user_mgr.add_role(&tenant, role_info, false).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
