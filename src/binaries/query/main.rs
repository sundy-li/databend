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

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]

mod local;

use std::env;
use std::sync::Arc;

use common_base::base::tokio;
use common_base::mem_allocator::GlobalAllocator;
use common_base::runtime::execute_futures_in_parallel;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_base::runtime::GLOBAL_MEM_STAT;
use common_base::set_alloc_error_hook;
use common_config::InnerConfig;
use common_config::DATABEND_COMMIT_VERSION;
use common_config::QUERY_SEMVER;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_client::MIN_METASRV_SEMVER;
use common_metrics::init_default_metrics_recorder;
use common_storage::DataOperator;
use common_tracing::set_panic_hook;
use databend_query::api::HttpService;
use databend_query::api::RpcService;
use databend_query::clusters::ClusterDiscovery;
use databend_query::metrics::MetricService;
use databend_query::servers::HttpHandler;
use databend_query::servers::HttpHandlerKind;
use databend_query::servers::MySQLHandler;
use databend_query::servers::Server;
use databend_query::servers::ShutdownHandle;
use databend_query::storages::fuse::statistics::memory_test;
use databend_query::GlobalServices;
use parking_lot::RwLock;
use tracing::info;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator;

fn main() {
    match Runtime::with_default_worker_threads() {
        Err(cause) => {
            eprintln!("Databend Query start failure, cause: {:?}", cause);
            std::process::exit(cause.code() as i32);
        }
        Ok(rt) => {
            if let Err(cause) = rt.block_on(main_entrypoint()) {
                eprintln!("Databend Query start failure, cause: {:?}", cause);
                std::process::exit(cause.code() as i32);
            }
        }
    }
}

async fn read_segment(data: Arc<RwLock<i32>>, data_op: DataOperator, i: i32) -> usize {
    let index = i % 100;
    let file_name = format!("tmp/p{index}.txt");

    let time = std::time::Instant::now();
    let content = data_op.operator().read(&file_name).await.unwrap();

    let mut data = data.write();
    *data += 1;

    content.len() as usize
}

async fn test(thread: usize, permits: usize) -> Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(
        2,
        Some("query-ctx".to_string()),
    )?);

    let ss: Vec<usize> = (0..100000).collect();

    let data = Arc::new(RwLock::new(0));

    for chunk in ss.chunks(thread) {
        let mut chunks = chunk.to_vec();
        let st = chunks[0];
        let data_op = common_storage::DataOperator::instance();

        let data = data.clone();

        let query_result = runtime.spawn(async move {
            let mut iter = chunks.iter();

            let futures = std::iter::from_fn(move || {
                if let Some(location) = iter.next() {
                    Some(read_segment(
                        data.clone(),
                        data_op.clone(),
                        *location as i32,
                    ))
                } else {
                    None
                }
            });

            let results: Vec<usize> = execute_futures_in_parallel(
                futures,
                thread,
                permits,
                "test-paralleled-futures".to_string(),
            )
            .await
            .unwrap();

            results
        });

        let query_result = query_result.await.unwrap();
        println!("read at: {:?} / {}", st, query_result.len());
    }
    Ok(())
}

async fn main_entrypoint() -> Result<()> {
    let conf: InnerConfig = InnerConfig::load()?;

    if run_cmd(&conf).await? {
        return Ok(());
    }

    init_default_metrics_recorder();
    set_panic_hook();
    set_alloc_error_hook();
    // Make sure global services have been inited.
    GlobalServices::init(conf.clone()).await?;

    // let _ = test(16, 16).await;

    let _ = memory_test();

    info!("Shutdown server.");
    Ok(())
}

async fn run_cmd(conf: &InnerConfig) -> Result<bool> {
    if conf.cmd.is_empty() {
        return Ok(false);
    }

    match conf.cmd.as_str() {
        "ver" => {
            println!("version: {}", *QUERY_SEMVER);
            println!("min-compatible-metasrv-version: {}", MIN_METASRV_SEMVER);
        }
        "local" => {
            println!("exec local query: {}", conf.local.sql);
            local::query_local(conf).await?
        }
        _ => {
            eprintln!("Invalid cmd: {}", conf.cmd);
            eprintln!("Available cmds:");
            eprintln!("  --cmd ver");
            eprintln!("    Print version and the min compatible databend-meta version");
        }
    }

    Ok(true)
}

#[cfg(not(target_os = "macos"))]
fn check_max_open_files() {
    let limits = match limits_rs::get_own_limits() {
        Ok(limits) => limits,
        Err(err) => {
            tracing::warn!("get system limit of databend-query failed: {:?}", err);
            return;
        }
    };
    let max_open_files_limit = limits.max_open_files.soft;
    if let Some(max_open_files) = max_open_files_limit {
        if max_open_files < 65535 {
            tracing::warn!(
                "The open file limit is too low for the databend-query. Please consider increase it by running `ulimit -n 65535`"
            );
        }
    }
}
