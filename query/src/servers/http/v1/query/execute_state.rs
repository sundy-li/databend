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
use std::time::Duration;
use std::time::Instant;

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::RwLock;
use common_base::ProgressValues;
use common_base::TrySpawn;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use ExecuteState::*;

use super::http_query::HttpQueryRequest;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;
use crate::sessions::SessionRef;
use crate::sql::PlanParser;

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum ExecuteStateKind {
    Running,
    Failed,
    Succeeded,
}

pub(crate) enum ExecuteState {
    Running(ExecuteRunning),
    Stopped(ExecuteStopped),
}

impl ExecuteState {
    pub(crate) fn extract(&self) -> (ExecuteStateKind, Option<ErrorCode>) {
        match self {
            ExecuteState::Running(_) => (ExecuteStateKind::Running, None),
            ExecuteState::Stopped(v) => match &v.reason {
                Ok(_) => (ExecuteStateKind::Succeeded, None),
                Err(e) => (ExecuteStateKind::Failed, Some(e.clone())),
            },
        }
    }
}

pub(crate) struct ExecuteRunning {
    // used to kill query
    session: SessionRef,
    // mainly used to get progress for now
    context: Arc<QueryContext>,
    interpreter: Arc<dyn Interpreter>,
}

pub(crate) struct ExecuteStopped {
    progress: Option<ProgressValues>,
    reason: Result<()>,
    stop_time: Instant,
}

pub(crate) struct Executor {
    start_time: Instant,
    pub(crate) state: ExecuteState,
}

impl Executor {
    pub(crate) fn get_progress(&self) -> Option<ProgressValues> {
        match &self.state {
            Running(r) => Some(r.context.get_scan_progress_value()),
            Stopped(f) => f.progress.clone(),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        match &self.state {
            Running(_) => Instant::now() - self.start_time,
            Stopped(f) => f.stop_time - self.start_time,
        }
    }

    pub(crate) async fn stop(this: &Arc<RwLock<Executor>>, reason: Result<()>, kill: bool) {
        let mut guard = this.write().await;
        if let Running(r) = &guard.state {
            // release session
            let progress = Some(r.context.get_scan_progress_value());
            if kill {
                r.session.force_kill_query();
            }
            // Write Finish to query log table.
            let _ = r
                .interpreter
                .finish()
                .await
                .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));
            guard.state = Stopped(ExecuteStopped {
                progress,
                reason,
                stop_time: Instant::now(),
            });
        };
    }
}

pub struct HttpQueryHandle {
    pub abort_sender: mpsc::Sender<()>,
}

impl HttpQueryHandle {
    pub fn abort(&self) {
        let sender = self.abort_sender.clone();
        tokio::spawn(async move {
            sender.send(()).await.ok();
        });
    }
}

impl ExecuteState {
    pub(crate) async fn try_create(
        request: &HttpQueryRequest,
        session: SessionRef,
        block_tx: mpsc::Sender<DataBlock>,
    ) -> Result<Arc<RwLock<Executor>>> {
        let sql = &request.sql;
        let start_time = Instant::now();
        let ctx = session.create_query_context().await?;
        ctx.attach_query_str(sql);
        let plan = PlanParser::parse(ctx.clone(), sql).await?;

        let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        // Write Start to query log table.
        let _ = interpreter
            .start()
            .await
            .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));

        let (abort_tx, mut abort_rx) = mpsc::channel(2);
        ctx.attach_http_query(HttpQueryHandle {
            abort_sender: abort_tx,
        });

        let running_state = ExecuteRunning {
            session,
            context: ctx.clone(),
            interpreter: interpreter.clone(),
        };
        let executor = Arc::new(RwLock::new(Executor {
            start_time,
            state: Running(running_state),
        }));

        let executor_clone = executor.clone();
        let ctx_clone = ctx.clone();
        ctx.try_spawn(async move {
            // drop/close block_tx after calling Executor::stop
            // so handler task can get newest state before return
            // otherwise the handler task and this task may competing for the executor lock
            let block_tx_clone = block_tx.clone();
            match execute(interpreter, ctx_clone, block_tx_clone, &mut abort_rx).await {
                Ok(_) => Executor::stop(&executor_clone, Ok(()), false).await,
                Err(err) => {
                    let kill = err.message().starts_with("aborted");
                    Executor::stop(&executor_clone, Err(err), kill).await
                }
            };
        })?;

        Ok(executor)
    }
}

async fn execute(
    interpreter: Arc<dyn Interpreter>,
    ctx: Arc<QueryContext>,
    block_tx: mpsc::Sender<DataBlock>,
    abort_rx: &mut mpsc::Receiver<()>,
) -> Result<()> {
    let data_stream = interpreter.execute(None, None).await?;
    let mut data_stream = ctx.try_create_abortable(data_stream)?;
    while let Some(block_r) = data_stream.next().await {
        match block_r {
            Ok(block) => tokio::select! {
                _ = block_tx.send(block) => { },
                _ = abort_rx.recv() => {
                    return Err(ErrorCode::AbortedQuery("aborted"))
                },
            },
            Err(err) => return Err(err),
        };
    }
    Ok(())
}
