//  Copyright 2021 Datafuse Labs.
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

use std::any::Any;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str;
use std::sync::Arc;

use common_catalog::catalog::StorageDescription;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Expression;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::AppendMode;
use common_catalog::table::ColumnId;
use common_catalog::table::ColumnStatistics;
use common_catalog::table::ColumnStatisticsProvider;
use common_catalog::table::CompactTarget;
use common_catalog::table::NavigationDescriptor;
use common_catalog::table_context::TableContext;
use common_catalog::table_mutator::TableMutator;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::TableInfo;
use common_sharing::create_share_table_operator;
use common_sql::ExpressionParser;
use common_storage::init_operator;
use common_storage::CacheOperator;
use common_storage::DataOperator;
use common_storage::FuseCachePolicy;
use common_storage::ShareTableConfig;
use common_storage::StorageMetrics;
use common_storage::StorageMetricsLayer;
use common_storages_table_meta::meta::ClusterKey;
use common_storages_table_meta::meta::ColumnStatistics as FuseColumnStatistics;
use common_storages_table_meta::meta::Statistics as FuseStatistics;
use common_storages_table_meta::meta::TableSnapshot;
use common_storages_table_meta::meta::TableSnapshotStatistics;
use common_storages_table_meta::meta::Versioned;
use common_storages_table_meta::table::table_storage_prefix;
use common_storages_table_meta::table::OPT_KEY_DATABASE_ID;
use common_storages_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use common_storages_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use opendal::layers::CacheLayer;
use opendal::Operator;
use uuid::Uuid;

use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::operations::AppendOperationLogEntry;
use crate::operations::ReadDataKind;
use crate::pipelines::Pipeline;
use crate::NavigationPoint;
use crate::Table;
use crate::TableStatistics;
use crate::DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD;
use crate::DEFAULT_ROW_PER_BLOCK;
use crate::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT;

#[derive(Clone)]
pub struct FuseTable {
    pub(crate) table_info: TableInfo,
    pub(crate) meta_location_generator: TableMetaLocationGenerator,

    pub(crate) cluster_key_meta: Option<ClusterKey>,
    pub(crate) read_only: bool,

    pub(crate) operator: Operator,
    pub(crate) data_metrics: Arc<StorageMetrics>,
}

impl FuseTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Self::do_create(table_info, false)?)
    }

    pub fn do_create(table_info: TableInfo, read_only: bool) -> Result<Box<FuseTable>> {
        let storage_prefix = Self::parse_storage_prefix(&table_info)?;
        let cluster_key_meta = table_info.meta.cluster_key();

        let mut operator = match table_info.db_type.clone() {
            DatabaseType::ShareDB(share_ident) => create_share_table_operator(
                ShareTableConfig::share_endpoint_address(),
                ShareTableConfig::share_endpoint_token(),
                &share_ident.tenant,
                &share_ident.share_name,
                &table_info.name,
            ),
            DatabaseType::NormalDB => {
                let storage_params = table_info.meta.storage_params.clone();
                match storage_params {
                    Some(sp) => init_operator(&sp)?,
                    None => DataOperator::instance().operator(),
                }
            }
        };

        let data_metrics = Arc::new(StorageMetrics::default());
        operator = operator.layer(StorageMetricsLayer::new(data_metrics.clone()));
        // If cache op is valid, layered with ContentCacheLayer.
        if let Some(cache_op) = CacheOperator::instance() {
            operator =
                operator.layer(CacheLayer::new(cache_op).with_policy(FuseCachePolicy::new()));
        }

        Ok(Box::new(FuseTable {
            table_info,
            meta_location_generator: TableMetaLocationGenerator::with_prefix(storage_prefix),
            cluster_key_meta,
            read_only,
            operator,
            data_metrics,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "FUSE".to_string(),
            comment: "FUSE Storage Engine".to_string(),
            support_cluster_key: true,
        }
    }

    pub fn meta_location_generator(&self) -> &TableMetaLocationGenerator {
        &self.meta_location_generator
    }

    pub fn parse_storage_prefix(table_info: &TableInfo) -> Result<String> {
        let table_id = table_info.ident.table_id;
        let db_id = table_info
            .options()
            .get(OPT_KEY_DATABASE_ID)
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Invalid fuse table, table option {} not found",
                    OPT_KEY_DATABASE_ID
                ))
            })?;
        Ok(table_storage_prefix(db_id, table_id))
    }

    pub fn table_snapshot_statistics_format_version(&self, location: &String) -> u64 {
        TableMetaLocationGenerator::snapshot_version(location)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn read_table_snapshot_statistics(
        &self,
        snapshot: Option<&Arc<TableSnapshot>>,
    ) -> Result<Option<Arc<TableSnapshotStatistics>>> {
        match snapshot {
            Some(snapshot) => {
                if let Some(loc) = &snapshot.table_statistics_location {
                    let ver = self.table_snapshot_statistics_format_version(loc);
                    let reader = MetaReaders::table_snapshot_statistics_reader(self.get_operator());
                    Ok(Some(reader.read(loc.as_str(), None, ver).await?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read_table_snapshot(&self) -> Result<Option<Arc<TableSnapshot>>> {
        if let Some(loc) = self.snapshot_loc().await? {
            let reader = MetaReaders::table_snapshot_reader(self.get_operator());
            let ver = self.snapshot_format_version().await?;
            Ok(Some(reader.read(loc.as_str(), None, ver).await?))
        } else {
            Ok(None)
        }
    }

    pub async fn snapshot_format_version(&self) -> Result<u64> {
        match self.snapshot_loc().await? {
            Some(loc) => Ok(TableMetaLocationGenerator::snapshot_version(loc.as_str())),
            None => {
                // No snapshot location here, indicates that there are no data of this table yet
                // in this case, we just returns the current snapshot version
                Ok(TableSnapshot::VERSION)
            }
        }
    }

    pub async fn snapshot_loc(&self) -> Result<Option<String>> {
        match self.table_info.db_type {
            DatabaseType::ShareDB(_) => {
                let url = FUSE_TBL_LAST_SNAPSHOT_HINT;
                let data = self.operator.object(url).read().await?;
                let s = str::from_utf8(&data)?;
                Ok(Some(s.to_string()))
            }
            DatabaseType::NormalDB => {
                let options = self.table_info.options();
                Ok(options
                    .get(OPT_KEY_SNAPSHOT_LOCATION)
                    // for backward compatibility, we check the legacy table option
                    .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))
                    .cloned())
            }
        }
    }

    pub fn get_operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn get_operator_ref(&self) -> &Operator {
        &self.operator
    }

    pub fn try_from_table(tbl: &dyn Table) -> Result<&FuseTable> {
        tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "expects table of engine FUSE, but got {}",
                tbl.engine()
            ))
        })
    }

    pub fn check_mutable(&self) -> Result<()> {
        if self.read_only {
            Err(ErrorCode::TableNotWritable(format!(
                "Table {} is in read-only mode",
                self.table_info.desc.as_str()
            )))
        } else {
            Ok(())
        }
    }

    pub fn transient(&self) -> bool {
        self.table_info.meta.options.contains_key("TRANSIENT")
    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn is_local(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        Some(self.data_metrics.clone())
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn cluster_keys(&self) -> Vec<Expression> {
        let table_meta = Arc::new(self.clone());
        if let Some((_, order)) = &self.cluster_key_meta {
            let cluster_keys = ExpressionParser::parse_exprs(table_meta, order).unwrap();
            return cluster_keys;
        }
        vec![]
    }

    fn support_prewhere(&self) -> bool {
        true
    }

    async fn alter_table_cluster_keys(
        &self,
        ctx: Arc<dyn TableContext>,
        cluster_key_str: String,
    ) -> Result<()> {
        let mut new_table_meta = self.get_table_info().meta.clone();
        new_table_meta = new_table_meta.push_cluster_key(cluster_key_str);
        let cluster_key_meta = new_table_meta.cluster_key();
        let schema = self.schema().as_ref().clone();

        let prev = self.read_table_snapshot().await?;
        let prev_version = self.snapshot_format_version().await?;
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let prev_snapshot_id = prev.as_ref().map(|v| (v.snapshot_id, prev_version));
        let prev_statistics_location = prev
            .as_ref()
            .and_then(|v| v.table_statistics_location.clone());
        let (summary, segments) = if let Some(v) = prev {
            (v.summary.clone(), v.segments.clone())
        } else {
            (FuseStatistics::default(), vec![])
        };

        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &prev_timestamp,
            prev_snapshot_id,
            schema,
            summary,
            segments,
            cluster_key_meta,
            prev_statistics_location,
        );

        let mut table_info = self.table_info.clone();
        table_info.meta = new_table_meta;

        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            &table_info,
            &self.meta_location_generator,
            new_snapshot,
            None,
            &self.operator,
        )
        .await
    }

    async fn drop_table_cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        if self.cluster_key_meta.is_none() {
            return Ok(());
        }

        let mut new_table_meta = self.get_table_info().meta.clone();
        new_table_meta.default_cluster_key = None;
        new_table_meta.default_cluster_key_id = None;

        let schema = self.schema().as_ref().clone();

        let prev = self.read_table_snapshot().await?;
        let prev_version = self.snapshot_format_version().await?;
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let prev_statistics_location = prev
            .as_ref()
            .and_then(|v| v.table_statistics_location.clone());
        let prev_snapshot_id = prev.as_ref().map(|v| (v.snapshot_id, prev_version));
        let (summary, segments) = if let Some(v) = prev {
            (v.summary.clone(), v.segments.clone())
        } else {
            (FuseStatistics::default(), vec![])
        };

        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &prev_timestamp,
            prev_snapshot_id,
            schema,
            summary,
            segments,
            None,
            prev_statistics_location,
        );

        let mut table_info = self.table_info.clone();
        table_info.meta = new_table_meta;

        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            &table_info,
            &self.meta_location_generator,
            new_snapshot,
            None,
            &self.operator,
        )
        .await
    }

    #[tracing::instrument(level = "debug", name = "fuse_table_read_partitions", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    #[tracing::instrument(level = "debug", name = "fuse_table_read_data", skip(self, ctx, pipeline), fields(ctx.id = ctx.get_id().as_str()))]
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline, ReadDataKind::BlockDataAdjustIORequests)
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
        need_output: bool,
    ) -> Result<()> {
        self.check_mutable()?;
        self.do_append_data(ctx, pipeline, append_mode, need_output)
    }

    #[tracing::instrument(level = "debug", name = "fuse_table_commit_insertion", skip(self, ctx, operations), fields(ctx.id = ctx.get_id().as_str()))]
    async fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        operations: Vec<DataBlock>,
        overwrite: bool,
    ) -> Result<()> {
        self.check_mutable()?;
        // only append operation supported currently
        let append_log_entries = operations
            .iter()
            .map(AppendOperationLogEntry::try_from)
            .collect::<Result<Vec<AppendOperationLogEntry>>>()?;
        self.do_commit(ctx, append_log_entries, overwrite).await
    }

    #[tracing::instrument(level = "debug", name = "fuse_table_truncate", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn truncate(&self, ctx: Arc<dyn TableContext>, purge: bool) -> Result<()> {
        self.check_mutable()?;
        self.do_truncate(ctx, purge).await
    }

    #[tracing::instrument(level = "debug", name = "fuse_table_optimize", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn purge(&self, ctx: Arc<dyn TableContext>, keep_last_snapshot: bool) -> Result<()> {
        self.check_mutable()?;
        self.do_purge(&ctx, keep_last_snapshot).await
    }

    #[tracing::instrument(level = "debug", name = "analyze", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        self.do_analyze(&ctx).await
    }

    fn table_statistics(&self) -> Result<Option<TableStatistics>> {
        let s = &self.table_info.meta.statistics;
        Ok(Some(TableStatistics {
            num_rows: Some(s.number_of_rows),
            data_size: Some(s.data_bytes),
            data_size_compressed: Some(s.compressed_data_bytes),
            index_size: Some(s.index_data_bytes),
        }))
    }

    async fn column_statistics_provider(&self) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let provider = if let Some(snapshot) = self.read_table_snapshot().await? {
            let stats = &snapshot.summary.col_stats;
            let table_statistics = self.read_table_snapshot_statistics(Some(&snapshot)).await?;
            if let Some(table_statistics) = table_statistics {
                FuseTableColumnStatisticsProvider {
                    column_stats: stats.clone(),
                    row_count: snapshot.summary.row_count,
                    // save row count first
                    column_distinct_values: Some(table_statistics.column_distinct_values.clone()),
                }
            } else {
                FuseTableColumnStatisticsProvider {
                    column_stats: stats.clone(),
                    row_count: snapshot.summary.row_count,
                    column_distinct_values: None,
                }
            }
        } else {
            FuseTableColumnStatisticsProvider::default()
        };
        Ok(Box::new(provider))
    }

    #[tracing::instrument(level = "debug", name = "fuse_table_navigate_to", skip_all)]
    async fn navigate_to(&self, point: &NavigationPoint) -> Result<Arc<dyn Table>> {
        match point {
            NavigationPoint::SnapshotID(snapshot_id) => {
                Ok(self.navigate_to_snapshot(snapshot_id.as_str()).await?)
            }
            NavigationPoint::TimePoint(time_point) => {
                Ok(self.navigate_to_time_point(*time_point).await?)
            }
        }
    }

    async fn delete(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<Expression>,
        col_indices: Vec<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_delete(ctx, filter, col_indices, pipeline).await
    }

    fn get_block_compact_thresholds(&self) -> BlockCompactThresholds {
        let max_rows_per_block = self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);
        let min_rows_per_block = (max_rows_per_block as f64 * 0.8) as usize;
        let max_bytes_per_block = self.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        );
        BlockCompactThresholds::new(max_rows_per_block, min_rows_per_block, max_bytes_per_block)
    }

    async fn compact(
        &self,
        ctx: Arc<dyn TableContext>,
        target: CompactTarget,
        limit: Option<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<bool> {
        self.do_compact(ctx, target, limit, pipeline).await
    }

    async fn recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<Box<dyn TableMutator>>> {
        self.do_recluster(ctx, pipeline, push_downs).await
    }

    async fn revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        point: NavigationDescriptor,
    ) -> Result<()> {
        // A read-only instance of fuse table, e.g. instance got by using time travel,
        // revert operation is not allowed.
        self.check_mutable()?;

        self.do_revert_to(ctx.as_ref(), point).await
    }
}

#[derive(Default)]
struct FuseTableColumnStatisticsProvider {
    column_stats: HashMap<ColumnId, FuseColumnStatistics>,
    pub column_distinct_values: Option<HashMap<ColumnId, u64>>,
    pub row_count: u64,
}

impl ColumnStatisticsProvider for FuseTableColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<ColumnStatistics> {
        let col_stats = &self.column_stats.get(&column_id);
        col_stats.map(|s| ColumnStatistics {
            min: s.min.clone(),
            max: s.max.clone(),
            null_count: s.null_count,
            number_of_distinct_values: self
                .column_distinct_values
                .as_ref()
                .map_or(self.row_count, |map| map.get(&column_id).map_or(0, |v| *v)),
        })
    }
}
