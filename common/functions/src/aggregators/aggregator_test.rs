// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[test]
fn test_aggregator_function() -> Result<()> {
    use std::sync::Arc;

    use common_datablocks::DataBlock;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::aggregators::*;
    use crate::arithmetics::*;
    use crate::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        eval_nums: usize,
        args: Vec<Box<dyn IFunction>>,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataValue,
        error: &'static str,
        func: Box<dyn IFunction>
    }

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]));
    let block = DataBlock::create(schema.clone(), vec![
        Arc::new(Int64Array::from(vec![4, 3, 2, 1, 1, 2, 3, 4])),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 4, 3, 2, 1])),
    ]);

    let field_a = ColumnFunction::try_create("a")?;
    let field_b = ColumnFunction::try_create("b")?;

    let tests = vec![
        Test {
            name: "count-passed",
            eval_nums: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "count(a)",
            nullable: false,
            func: AggregatorCountFunction::try_create("count", &[ColumnFunction::try_create(
                "a"
            )?])?,
            block: block.clone(),
            expect: DataValue::UInt64(Some(8)),
            error: ""
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "max(a)",
            nullable: false,
            func: AggregatorMaxFunction::try_create("max", &[ColumnFunction::try_create("a")?])?,
            block: block.clone(),
            expect: DataValue::Int64(Some(4)),
            error: ""
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "min(a)",
            nullable: false,
            func: AggregatorMinFunction::try_create("min", &[ColumnFunction::try_create("a")?])?,
            block: block.clone(),
            expect: DataValue::Int64(Some(1)),
            error: ""
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "avg(a)",
            nullable: false,
            func: AggregatorAvgFunction::try_create("avg", &[ColumnFunction::try_create("a")?])?,
            block: block.clone(),
            expect: DataValue::Float64(Some(2.5)),
            error: ""
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "sum(a)",
            nullable: false,
            func: AggregatorSumFunction::try_create("sum", &[ColumnFunction::try_create("a")?])?,
            block: block.clone(),
            expect: DataValue::Int64(Some(20)),
            error: ""
        },
        Test {
            name: "1+1+sum(a)-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "plus(1, plus(1, sum(a)))",
            nullable: false,
            func: ArithmeticPlusFunction::try_create_func("+", &[
                LiteralFunction::try_create(DataValue::Int64(Some(1)))?,
                ArithmeticPlusFunction::try_create_func("+", &[
                    LiteralFunction::try_create(DataValue::Int64(Some(1)))?,
                    AggregatorSumFunction::try_create("sum", &[ColumnFunction::try_create("a")?])?
                ])?
            ])?,
            block: block.clone(),
            expect: DataValue::Int64(Some(72)),
            error: ""
        },
        Test {
            name: "sum(a)/count(a)-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "divide(sum(a), count(a))",
            nullable: false,
            func: ArithmeticDivFunction::try_create_func("/", &[
                AggregatorSumFunction::try_create("sum", &[ColumnFunction::try_create("a")?])?,
                AggregatorCountFunction::try_create("count", &[ColumnFunction::try_create("a")?])?
            ])?,
            block: block.clone(),
            expect: DataValue::Float64(Some(2.5)),
            error: ""
        },
        Test {
            name: "(sum(a+1)+2)-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "plus(sum(plus(a, 1)), 2)",
            nullable: false,
            func: ArithmeticPlusFunction::try_create_func("+", &[
                AggregatorSumFunction::try_create("sum", &[
                    ArithmeticPlusFunction::try_create_func("+", &[
                        ColumnFunction::try_create("a")?,
                        LiteralFunction::try_create(DataValue::Int8(Some(1)))?
                    ])?
                ])?,
                LiteralFunction::try_create(DataValue::Int8(Some(2)))?
            ])?,
            block: block.clone(),
            expect: DataValue::Int64(Some(100)),
            error: ""
        },
        Test {
            name: "(uniq(a+1)+2)-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "plus(uniq(plus(a, 1)), 2)",
            nullable: false,
            func: ArithmeticPlusFunction::try_create_func("+", &[
                AggregatorUniqFunction::try_create("uniq", &[
                    ArithmeticPlusFunction::try_create_func("+", &[
                        ColumnFunction::try_create("a")?,
                        LiteralFunction::try_create(DataValue::Int8(Some(1)))?
                    ])?
                ])?,
                LiteralFunction::try_create(DataValue::Int8(Some(2)))?
            ])?,
            block: block.clone(),
            expect: DataValue::Int64(Some(6)),
            error: ""
        },
    ];

    for t in tests {
        let mut func1 = t.func.clone();
        for _ in 0..t.eval_nums {
            func1.accumulate(&t.block)?;
        }
        let state1 = func1.accumulate_result()?;

        let mut func2 = t.func.clone();
        for _ in 1..t.eval_nums {
            func2.accumulate(&t.block)?;
        }
        let state2 = func2.accumulate_result()?;

        let mut final_func = t.func.clone();
        final_func.set_depth(0);
        final_func.merge(&*state1)?;
        final_func.merge(&*state2)?;

        let result = final_func.merge_result()?;

        assert_eq!(&t.expect, &result);
        assert_eq!(t.display, format!("{:}", final_func));
    }
    Ok(())
}
