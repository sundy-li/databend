// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_limit_plan() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;
    use std::sync::Arc;

    use crate::planners::*;

    let ctx = crate::tests::try_create_context()?;

    let limit = PlanNode::Limit(LimitPlan {
        n: 33,
        input: Arc::from(PlanBuilder::empty(ctx).build()?),
    });
    let expect = "Limit: 33\n  ";
    let actual = format!("{:?}", limit);
    assert_eq!(expect, actual);
    Ok(())
}
