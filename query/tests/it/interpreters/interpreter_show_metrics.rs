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

use common_base::tokio;
use common_exception::Result;
use common_metrics::init_default_metrics_recorder;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_metrics_interpreter() -> Result<()> {
    init_default_metrics_recorder();
    let ctx = crate::tests::create_query_context().await?;

    // show metrics.
    {
        let plan = PlanParser::parse(ctx.clone(), "show metrics").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowMetricsInterpreter");
        let _ = executor.execute(None, None).await?;
    }

    Ok(())
}
