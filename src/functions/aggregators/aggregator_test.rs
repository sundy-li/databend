// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_aggregator_function() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datablocks::DataBlock;
    use crate::datavalues::*;
    use crate::functions::aggregators::*;
    use crate::functions::arithmetics::*;
    use crate::functions::*;

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
        func: Box<dyn IFunction>,
    }

    let ctx = crate::tests::try_create_context()?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]));
    let block = DataBlock::create(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        ],
    );

    let field_a = FieldFunction::try_create("a")?;
    let field_b = FieldFunction::try_create("b")?;

    let tests = vec![
        Test {
            name: "count-passed",
            eval_nums: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Count(a)",
            nullable: false,
            func: AggregatorCountFunction::try_create(
                ctx.clone(),
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Max(a)",
            nullable: false,
            func: AggregatorMaxFunction::try_create(
                ctx.clone(),
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(4)),
            error: "",
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Min(a)",
            nullable: false,
            func: AggregatorMinFunction::try_create(
                ctx.clone(),
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Avg(a)",
            nullable: false,
            func: AggregatorAvgFunction::try_create(
                ctx.clone(),
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Float64(Some(2.5)),
            error: "",
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a)",
            nullable: false,
            func: AggregatorSumFunction::try_create(
                ctx.clone(),
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(10)),
            error: "",
        },
        Test {
            name: "sum(a)+1-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a) + 1",
            nullable: false,
            func: ArithmeticPlusFunction::try_create_func(
                ctx.clone(),
                &[
                    AggregatorSumFunction::try_create(
                        ctx.clone(),
                        &[FieldFunction::try_create("a")?],
                    )?,
                    ConstantFunction::try_create(DataValue::Int64(Some(1)))?,
                ],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(71)),
            error: "",
        },
        Test {
            name: "sum(a)/count(a)-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a)/Count(a)",
            nullable: false,
            func: ArithmeticDivFunction::try_create_func(
                ctx.clone(),
                &[
                    AggregatorSumFunction::try_create(
                        ctx.clone(),
                        &[FieldFunction::try_create("a")?],
                    )?,
                    AggregatorCountFunction::try_create(
                        ctx.clone(),
                        &[FieldFunction::try_create("a")?],
                    )?,
                ],
            )?,
            block: block.clone(),
            expect: DataValue::Float64(Some(2.5)),
            error: "",
        },
        Test {
            name: "(sum(a+1)+2)-merge-passed",
            eval_nums: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a+1)+2",
            nullable: false,
            func: ArithmeticPlusFunction::try_create_func(
                ctx.clone(),
                &[
                    AggregatorSumFunction::try_create(
                        ctx.clone(),
                        &[ArithmeticPlusFunction::try_create_func(
                            ctx,
                            &[
                                FieldFunction::try_create("a")?,
                                ConstantFunction::try_create(DataValue::Int8(Some(1)))?,
                            ],
                        )?],
                    )?,
                    ConstantFunction::try_create(DataValue::Int8(Some(2)))?,
                ],
            )?,
            block,
            expect: DataValue::Int64(Some(100)),
            error: "",
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
    }
    Ok(())
}
