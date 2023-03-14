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

use std::sync::Arc;

use chrono::Utc;
use common_exception::Result;
use common_expression::types::decimal::DecimalSize;
use common_expression::types::DecimalDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_hive_meta_store as hms;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_sql::resolve_type_name_by_str;

use crate::hive_catalog::HIVE_CATALOG;
use crate::hive_database::HiveDatabase;
use crate::hive_database::HIVE_DATABASE_ENGIE;
use crate::hive_table::HIVE_TABLE_ENGIE;
use crate::hive_table_options::HiveTableOptions;

/// ! Skeleton of mappers
impl From<hms::Database> for HiveDatabase {
    fn from(hms_database: hms::Database) -> Self {
        HiveDatabase {
            database_info: DatabaseInfo {
                ident: DatabaseIdent { db_id: 0, seq: 0 },
                name_ident: DatabaseNameIdent {
                    tenant: "TODO".to_owned(),
                    db_name: hms_database.name.unwrap_or_default(),
                },
                meta: DatabaseMeta {
                    engine: HIVE_DATABASE_ENGIE.to_owned(),
                    created_on: Utc::now(),
                    ..Default::default()
                },
            },
        }
    }
}

pub fn try_into_table_info(
    hms_table: hms::Table,
    fields: Vec<hms::FieldSchema>,
) -> Result<TableInfo> {
    let partition_keys = if let Some(partitions) = &hms_table.partition_keys {
        let r = partitions
            .iter()
            .filter_map(|field| field.name.clone())
            .collect();
        Some(r)
    } else {
        None
    };
    let schema = Arc::new(try_into_schema(fields)?);

    let location = if let Some(storage) = &hms_table.sd {
        storage
            .location
            .as_ref()
            .map(|location| location.to_string())
    } else {
        None
    };

    let table_options = HiveTableOptions {
        partition_keys,
        location,
    };

    let meta = TableMeta {
        schema,
        catalog: HIVE_CATALOG.to_string(),
        engine: HIVE_TABLE_ENGIE.to_owned(),
        engine_options: table_options.into(),
        created_on: Utc::now(),
        ..Default::default()
    };

    let real_name = format!(
        "{}.{}",
        hms_table.db_name.clone().unwrap_or_default(),
        hms_table.table_name.clone().unwrap_or_default()
    );

    let table_info = TableInfo {
        ident: TableIdent {
            table_id: 0,
            seq: 0,
        },
        desc: real_name,
        name: hms_table.table_name.unwrap_or_default(),
        meta,
        ..Default::default()
    };

    Ok(table_info)
}

fn try_into_schema(hive_fields: Vec<hms::FieldSchema>) -> Result<TableSchema> {
    let mut fields = Vec::new();
    for field in hive_fields {
        let name = field.name.unwrap_or_default();
        let type_name = field.type_.unwrap_or_default();

        let table_type = try_from_filed_type_name(type_name)?;
        let table_type = table_type.wrap_nullable();
        let field = TableField::new(&name, table_type);
        fields.push(field);
    }
    Ok(TableSchema::new(fields))
}

fn try_from_filed_type_name(type_name: impl AsRef<str>) -> Result<TableDataType> {
    let name = type_name.as_ref().to_uppercase();
    // TODO more mappings goes here
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
    // Hive string data type could be varchar(n), where n is the maximum number of characters
    if name.starts_with("VARCHAR") {
        Ok(TableDataType::String)
    } else if name.starts_with("ARRAY<") {
        let sub_type = &name["ARRAY<".len()..name.len() - 1];
        let sub_type = try_from_filed_type_name(sub_type)?;
        Ok(TableDataType::Array(Box::new(sub_type.wrap_nullable())))
    } else {
        match name.as_str() {
            "DECIMAL" | "NUMERIC" => Ok(TableDataType::Decimal(DecimalDataType::Decimal128(
                DecimalSize {
                    precision: 10,
                    scale: 0,
                },
            ))),
            _ => resolve_type_name_by_str(name.as_str()),
        }
    }
}
