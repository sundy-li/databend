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
//

use common_base::tokio;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::PlanParser;
use databend_query::storages::ToReadDataSourcePlan;
use databend_query::table_functions::NumbersTable;
use futures::TryStreamExt;

#[tokio::test]
async fn test_number_table() -> Result<()> {
    let tbl_args = Some(vec![Expression::create_literal(DataValue::UInt64(8))]);
    let ctx = crate::tests::create_query_context().await?;
    let table = NumbersTable::create("system", "numbers_mt", 1, tbl_args)?;

    let source_plan = table
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(Extras::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "| 3      |",
        "| 4      |",
        "| 5      |",
        "| 6      |",
        "| 7      |",
        "+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_limit_push_down() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
        result: Vec<&'static str>,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "only-limit",
            query: "select * from numbers_mt(10) limit 2",
            expect: "\
            Limit: 2\
            \n  Projection: number:UInt64\
            \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 2, read_bytes: 16, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], limit: 2]",
            result:
            vec![
                    "+--------+",
                    "| number |",
                    "+--------+",
                    "| 0      |",
                    "| 1      |",
                    "+--------+",
            ],
        },
        Test {
            name: "limit-with-filter",
            query: "select * from numbers_mt(10) where number > 8 limit 2",
            expect: "\
            Limit: 2\
            \n  Projection: number:UInt64\
            \n    Filter: (number > 8)\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [(number > 8)], limit: 2]",
            result:
                vec![
                    "+--------+",
                    "| number |",
                    "+--------+",
                    "| 9      |",
                    "+--------+",
                ],
        },
    ];

    for test in tests {
        let ctx = crate::tests::create_query_context().await?;
        let plan = PlanParser::parse(ctx.clone(), test.query).await?;
        let actual = format!("{:?}", plan);
        assert_eq!(test.expect, actual, "{:#?}", test.name);

        let executor = InterpreterFactory::get(ctx.clone(), plan)?;

        let stream = executor.execute(None, None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expect = test.result;
        let actual = result.as_slice();
        common_datablocks::assert_blocks_sorted_eq_with_name(test.name, expect, actual);
    }
    Ok(())
}
