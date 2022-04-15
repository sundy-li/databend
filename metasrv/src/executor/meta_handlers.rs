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

use std::convert::TryInto;
use std::sync::Arc;

use common_meta_grpc::GetTableExtReq;
use common_meta_types::AddResult;
use common_meta_types::AppError;
use common_meta_types::Change;
use common_meta_types::Cmd::CreateDatabase;
use common_meta_types::Cmd::CreateShare;
use common_meta_types::Cmd::CreateTable;
use common_meta_types::Cmd::DropDatabase;
use common_meta_types::Cmd::DropShare;
use common_meta_types::Cmd::DropTable;
use common_meta_types::Cmd::RenameTable;
use common_meta_types::Cmd::UpsertTableOptions;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateShareReply;
use common_meta_types::CreateShareReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseAlreadyExists;
use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropShareReply;
use common_meta_types::DropShareReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetShareReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::LogEntry;
use common_meta_types::MetaError;
use common_meta_types::OkOrExist;
use common_meta_types::RenameTableReply;
use common_meta_types::RenameTableReq;
use common_meta_types::ShareAlreadyExists;
use common_meta_types::ShareInfo;
use common_meta_types::TableAlreadyExists;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::TableVersionMismatched;
use common_meta_types::UnknownDatabase;
use common_meta_types::UnknownShare;
use common_meta_types::UnknownTable;
use common_meta_types::UnknownTableId;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<CreateDatabaseReq> for ActionHandler {
    async fn handle(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply, MetaError> {
        let db_name = req.db_name.clone();
        let if_not_exists = req.if_not_exists;

        let cr = LogEntry {
            txid: None,
            cmd: CreateDatabase(req),
        };

        let res = self.meta_node.write(cr).await?;

        let mut ch: Change<DatabaseMeta> = res
            .try_into()
            .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
        let db_id = ch.ident.take().expect("Some(db_id)");
        let (prev, _result) = ch.unpack_data();

        if prev.is_some() && !if_not_exists {
            let ae = AppError::from(DatabaseAlreadyExists::new(
                db_name,
                "RequestHandler: create_database",
            ));

            return Err(MetaError::from(ae));
        }

        Ok(CreateDatabaseReply {
            // TODO(xp): return DatabaseInfo?
            database_id: db_id,
        })
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetDatabaseReq> for ActionHandler {
    async fn handle(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, MetaError> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropDatabaseReq> for ActionHandler {
    async fn handle(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, MetaError> {
        let db_name = req.db_name.clone();
        let if_exists = req.if_exists;
        let cr = LogEntry {
            txid: None,
            cmd: DropDatabase(req),
        };

        let res = self.meta_node.write(cr).await?;

        let ch: Change<DatabaseMeta> = res
            .try_into()
            .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
        let (prev, _result) = ch.unpack_data();

        if prev.is_some() || if_exists {
            Ok(DropDatabaseReply {})
        } else {
            let ae = AppError::from(UnknownDatabase::new(
                db_name,
                "RequestHandler: drop_database",
            ));

            Err(MetaError::from(ae))
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<CreateTableReq> for ActionHandler {
    async fn handle(&self, req: CreateTableReq) -> Result<CreateTableReply, MetaError> {
        let db_name = req.db_name.clone();
        let table_name = req.table_name.clone();
        let if_not_exists = req.if_not_exists;

        tracing::info!("create table: {:}: {:?}", &db_name, &table_name);

        let cr = LogEntry {
            txid: None,
            cmd: CreateTable(req),
        };

        let rst = self.meta_node.write(cr).await?;

        let add_res: AddResult<TableMeta, u64> = rst.try_into()?;

        if let OkOrExist::Exists(_) = add_res.res {
            if !if_not_exists {
                let ae = AppError::from(TableAlreadyExists::new(
                    table_name,
                    "RequestHandler: create_table",
                ));

                return Err(MetaError::from(ae));
            }
        }

        Ok(CreateTableReply {
            // safe unwrap: id is not None.
            table_id: add_res.id.unwrap(),
        })
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropTableReq> for ActionHandler {
    async fn handle(&self, req: DropTableReq) -> Result<DropTableReply, MetaError> {
        let table_name = req.table_name.clone();
        let if_exists = req.if_exists;

        let cr = LogEntry {
            txid: None,
            cmd: DropTable(req),
        };

        let res = self.meta_node.write(cr).await?;

        let ch: Change<TableMeta> = res
            .try_into()
            .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
        let (prev, _result) = ch.unpack();

        if prev.is_some() || if_exists {
            Ok(DropTableReply {})
        } else {
            let ae = AppError::from(UnknownTable::new(table_name, "RequestHandler: drop_table"));

            Err(MetaError::from(ae))
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<RenameTableReq> for ActionHandler {
    async fn handle(&self, req: RenameTableReq) -> Result<RenameTableReply, MetaError> {
        let if_exists = req.if_exists;

        let cr = LogEntry {
            txid: None,
            cmd: RenameTable(req),
        };

        let res = self.meta_node.write(cr).await;

        if let Err(MetaError::AppError(AppError::UnknownTable(e))) = res {
            if if_exists {
                Ok(RenameTableReply { table_id: 0 })
            } else {
                Err(MetaError::AppError(AppError::UnknownTable(e)))
            }
        } else {
            let mut ch: Change<TableMeta> = res?
                .try_into()
                .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
            let table_id = ch.ident.take().unwrap();
            Ok(RenameTableReply { table_id })
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableReq> for ActionHandler {
    async fn handle(&self, req: GetTableReq) -> Result<Arc<TableInfo>, MetaError> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableExtReq> for ActionHandler {
    async fn handle(&self, act: GetTableExtReq) -> Result<TableInfo, MetaError> {
        // TODO duplicated code
        let table_id = act.tbl_id;
        let result = self.meta_node.get_table_by_id(&table_id).await?;
        match result {
            Some(table) => Ok(TableInfo::new(
                "",
                "",
                TableIdent::new(table_id, table.seq),
                table.data,
            )),
            None => {
                let ae = AppError::from(UnknownTableId::new(
                    act.tbl_id,
                    "RequestHandler: get_table_ext",
                ));

                Err(MetaError::from(ae))
            }
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<ListDatabaseReq> for ActionHandler {
    async fn handle(&self, req: ListDatabaseReq) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl RequestHandler<ListTableReq> for ActionHandler {
    async fn handle(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl RequestHandler<UpsertTableOptionReq> for ActionHandler {
    async fn handle(&self, req: UpsertTableOptionReq) -> Result<UpsertTableOptionReply, MetaError> {
        let cr = LogEntry {
            txid: None,
            cmd: UpsertTableOptions(req.clone()),
        };

        let res = self.meta_node.write(cr).await?;

        if !res.changed() {
            let ch: Change<TableMeta> = res
                .try_into()
                .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
            // safe unwrap: res not changed, so `prev` and `result` are not None.
            let (prev, _result) = ch.unwrap();

            let ae = AppError::from(TableVersionMismatched::new(
                req.table_id,
                req.seq,
                prev.seq,
                "RequestHandler: upsert_table_option",
            ));

            return Err(MetaError::from(ae));
        }

        Ok(UpsertTableOptionReply {})
    }
}

#[async_trait::async_trait]
impl RequestHandler<CreateShareReq> for ActionHandler {
    async fn handle(&self, req: CreateShareReq) -> Result<CreateShareReply, MetaError> {
        let share_name = &req.share_name;
        let if_not_exists = req.if_not_exists;

        let cr = LogEntry {
            txid: None,
            cmd: CreateShare(req.clone()),
        };

        let res = self.meta_node.write(cr).await?;

        let mut ch: Change<ShareInfo> = res
            .try_into()
            .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
        let share_id = ch.ident.take().expect("Some(share_id)");
        let (prev, _result) = ch.unpack_data();

        if prev.is_some() && !if_not_exists {
            let ae = AppError::from(ShareAlreadyExists::new(
                share_name,
                "RequestHandler: create_share",
            ));

            return Err(MetaError::from(ae));
        }

        Ok(CreateShareReply { share_id })
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropShareReq> for ActionHandler {
    async fn handle(&self, req: DropShareReq) -> Result<DropShareReply, MetaError> {
        let share_name = req.share_name.clone();
        let if_exists = req.if_exists;
        let cr = LogEntry {
            txid: None,
            cmd: DropShare(req),
        };

        let res = self.meta_node.write(cr).await?;

        let ch: Change<ShareInfo> = res
            .try_into()
            .map_err(|e: &str| MetaError::MetaServiceError(e.to_string()))?;
        let (prev, _result) = ch.unpack_data();

        if prev.is_some() || if_exists {
            Ok(DropShareReply {})
        } else {
            let ae = AppError::from(UnknownShare::new(share_name, "RequestHandler: drop_share"));

            Err(MetaError::from(ae))
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetShareReq> for ActionHandler {
    async fn handle(&self, req: GetShareReq) -> Result<Arc<ShareInfo>, MetaError> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}
