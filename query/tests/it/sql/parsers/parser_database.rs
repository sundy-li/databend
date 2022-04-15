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

use std::collections::BTreeMap;

use common_exception::Result;
use databend_query::sql::statements::DfCreateDatabase;
use databend_query::sql::statements::DfDropDatabase;
use databend_query::sql::statements::DfShowCreateDatabase;
use databend_query::sql::*;
use sqlparser::ast::*;

use crate::sql::sql_parser::*;

#[test]
fn create_database() -> Result<()> {
    {
        let sql = "CREATE DATABASE db1";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
        });
        expect_parse_ok(sql, expected)?;
    }
    expect_synonym_parse_eq("CREATE DATABASE db1", "CREATE SCHEMA db1")?;

    {
        let sql = "CREATE DATABASE db1 engine = github";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: false,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "github".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
        });
        expect_parse_ok(sql, expected)?;
    }
    expect_synonym_parse_eq(
        "CREATE DATABASE db1 engine = github",
        "CREATE SCHEMA db1 engine = github",
    )?;

    {
        let sql = "CREATE DATABASE IF NOT EXISTS db1";
        let expected = DfStatement::CreateDatabase(DfCreateDatabase {
            if_not_exists: true,
            name: ObjectName(vec![Ident::new("db1")]),
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
        });
        expect_parse_ok(sql, expected)?;
    }
    expect_synonym_parse_eq(
        "CREATE DATABASE IF NOT EXISTS db1",
        "CREATE SCHEMA IF NOT EXISTS db1",
    )?;

    Ok(())
}

#[test]
fn drop_database() -> Result<()> {
    {
        let sql = "DROP DATABASE db1";
        let expected = DfStatement::DropDatabase(DfDropDatabase {
            if_exists: false,
            name: ObjectName(vec![Ident::new("db1")]),
        });
        expect_parse_ok(sql, expected)?;
    }
    expect_synonym_parse_eq("DROP DATABASE db1", "DROP SCHEMA db1")?;

    {
        let sql = "DROP DATABASE IF EXISTS db1";
        let expected = DfStatement::DropDatabase(DfDropDatabase {
            if_exists: true,
            name: ObjectName(vec![Ident::new("db1")]),
        });
        expect_parse_ok(sql, expected)?;
    }
    expect_synonym_parse_eq("DROP DATABASE IF EXISTS db1", "DROP SCHEMA IF EXISTS db1")?;

    Ok(())
}

#[test]
fn show_create_database_test() -> Result<()> {
    expect_parse_ok(
        "SHOW CREATE DATABASE test",
        DfStatement::ShowCreateDatabase(DfShowCreateDatabase {
            name: ObjectName(vec![Ident::new("test")]),
        }),
    )?;
    expect_synonym_parse_eq("SHOW CREATE DATABASE test", "SHOW CREATE SCHEMA test")?;

    Ok(())
}
