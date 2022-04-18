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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::PasswordHashMethod;
use common_meta_types::RoleInfo;
use common_meta_types::UserGrantSet;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_revoke_privilege_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant().to_string();

    let name = "test";
    let hostname = "localhost";
    let password = "test";
    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(password),
        hash_method: PasswordHashMethod::PlainText,
    };
    let user_info = UserInfo::new(name.to_string(), hostname.to_string(), auth_info);
    assert_eq!(user_info.grants, UserGrantSet::empty());
    let user_mgr = ctx.get_user_manager();
    user_mgr.add_user(&tenant, user_info.clone(), false).await?;
    let query = format!("REVOKE ALL ON *.* FROM '{}'@'{}'", name, hostname);
    let plan = PlanParser::parse(ctx.clone(), &query).await?;
    let executor = InterpreterFactory::get(ctx, plan.clone())?;
    assert_eq!(executor.name(), "RevokePrivilegeInterpreter");
    let mut stream = executor.execute(None).await?;
    while let Some(_block) = stream.next().await {}
    let new_user = user_mgr.get_user(&tenant, user_info.identity()).await?;
    assert_eq!(new_user.grants, UserGrantSet::empty());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_revoke_privilege_interpreter_on_role() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant().to_string();

    let mut role_info = RoleInfo::new("role1".to_string());
    role_info
        .grants
        .grant_privileges(&GrantObject::Global, UserPrivilegeSet::all_privileges());
    let user_mgr = ctx.get_user_manager();
    assert!(role_info
        .grants
        .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));
    user_mgr.add_role(&tenant, role_info, false).await?;

    let query = "REVOKE ALL ON *.* FROM ROLE 'role1'";
    let plan = PlanParser::parse(ctx.clone(), query).await?;
    let executor = InterpreterFactory::get(ctx, plan.clone())?;
    assert_eq!(executor.name(), "RevokePrivilegeInterpreter");
    let mut stream = executor.execute(None).await?;
    while let Some(_block) = stream.next().await {}

    let role = user_mgr.get_role(&tenant, "role1".to_string()).await?;
    assert!(!role
        .grants
        .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));

    Ok(())
}
