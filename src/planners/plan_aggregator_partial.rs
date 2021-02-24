// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::{ExpressionPlan, PlanNode};

#[derive(Clone)]
pub struct AggregatorPartialPlan {
    pub group_expr: Vec<ExpressionPlan>,
    pub aggr_expr: Vec<ExpressionPlan>,
    pub input: Arc<PlanNode>,
}

impl AggregatorPartialPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn input(&self) -> Arc<PlanNode> {
        self.input.clone()
    }

    pub fn set_input(&mut self, input: &PlanNode) -> FuseQueryResult<()> {
        self.input = Arc::new(input.clone());
        Ok(())
    }
}
