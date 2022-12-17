//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::SystemTime;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_io::prelude::FormatSettings;
use common_meta_types::RoleInfo;
use common_meta_types::UserInfo;
use common_settings::Settings;
use common_storage::DataOperator;
use common_storage::StorageMetrics;

use crate::catalog::Catalog;
use crate::cluster_info::Cluster;
use crate::plan::DataSourcePlan;
use crate::plan::PartInfoPtr;
use crate::plan::Partitions;
use crate::table::Table;

#[derive(Debug)]
pub struct ProcessInfo {
    pub id: String,
    pub typ: String,
    pub state: String,
    pub database: String,
    pub user: Option<UserInfo>,
    pub settings: Arc<Settings>,
    pub client_address: Option<SocketAddr>,
    pub session_extra_info: Option<String>,
    pub memory_usage: i64,
    /// storage metrics for persisted data reading.
    pub data_metrics: Option<StorageMetrics>,
    pub scan_progress_value: Option<ProgressValues>,
    pub mysql_connection_id: Option<u32>,
    pub created_time: SystemTime,
}

#[derive(Debug, Clone)]
pub struct StageAttachment {
    pub location: String,
    pub format_options: BTreeMap<String, String>,
    pub copy_options: BTreeMap<String, String>,
}

#[async_trait::async_trait]
pub trait TableContext: Send + Sync {
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>>;
    fn get_scan_progress(&self) -> Arc<Progress>;
    fn get_scan_progress_value(&self) -> ProgressValues;
    fn get_write_progress(&self) -> Arc<Progress>;
    fn get_write_progress_value(&self) -> ProgressValues;
    fn get_result_progress(&self) -> Arc<Progress>;
    fn get_result_progress_value(&self) -> ProgressValues;
    fn try_get_part(&self) -> Option<PartInfoPtr>;
    // Update the context partition pool from the pipeline builder.
    fn try_set_partitions(&self, partitions: Partitions) -> Result<()>;
    fn attach_query_str(&self, kind: String, query: &str);
    fn get_fragment_id(&self) -> usize;
    fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>>;
    fn get_id(&self) -> String;
    fn get_current_catalog(&self) -> String;
    fn get_aborting(&self) -> Arc<AtomicBool>;
    fn get_current_database(&self) -> String;
    fn get_current_user(&self) -> Result<UserInfo>;
    fn get_current_role(&self) -> Option<RoleInfo>;
    fn get_fuse_version(&self) -> String;
    fn get_changed_settings(&self) -> Arc<Settings>;
    fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()>;
    fn get_format_settings(&self) -> Result<FormatSettings>;
    fn get_tenant(&self) -> String;
    /// Get the session running query.
    fn get_query_str(&self) -> String;
    /// Get the kind of session running query.
    fn get_query_kind(&self) -> String;
    // Get the persist storage data accessor operator from the session manager.
    fn get_data_operator(&self) -> Result<DataOperator>;
    fn push_precommit_block(&self, block: DataBlock);
    fn consume_precommit_blocks(&self) -> Vec<DataBlock>;
    fn try_get_function_context(&self) -> Result<FunctionContext>;
    fn get_connection_id(&self) -> String;
    fn get_settings(&self) -> Arc<Settings>;
    fn get_cluster(&self) -> Arc<Cluster>;
    async fn get_table(&self, catalog: &str, database: &str, table: &str)
    -> Result<Arc<dyn Table>>;
    fn get_processes_info(&self) -> Vec<ProcessInfo>;
    fn get_stage_attachment(&self) -> Option<StageAttachment>;
}
