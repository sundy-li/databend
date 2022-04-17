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
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::stream::StreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_setting_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    let plan = PlanParser::parse(ctx.clone(), "SET max_block_size=1").await?;
    let executor = InterpreterFactory::get(ctx.clone(), plan)?;
    assert_eq!(executor.name(), "SettingInterpreter");

    let mut stream = executor.execute(None, None).await?;
    while let Some(_block) = stream.next().await {}

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_setting_interpreter_error() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    let plan = PlanParser::parse(ctx.clone(), "SET max_block_size=1").await?;
    let executor = InterpreterFactory::get(ctx.clone(), plan)?;
    if let Err(e) = executor.execute(None, None).await {
        let expect = "Code: 1020, displayText = Unknown variable: \"xx\".";
        assert_eq!(expect, format!("{}", e));
    }

    Ok(())
}
