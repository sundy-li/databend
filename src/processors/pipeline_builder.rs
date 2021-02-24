// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{walk_postorder, PlanNode};
use crate::processors::{Pipeline, MergingSortedProcessor};
use crate::sessions::FuseQueryContextRef;
use crate::transforms::{
    AggregatorFinalTransform, AggregatorPartialTransform, FilterTransform, LimitTransform,
    MergingSortTransform, PartialSortTransform, ProjectionTransform, SourceTransform,
};

pub struct PipelineBuilder {
    ctx: FuseQueryContextRef,
    plan: PlanNode,
}

impl PipelineBuilder {
    pub fn create(ctx: FuseQueryContextRef, plan: PlanNode) -> Self {
        PipelineBuilder { ctx, plan }
    }

    pub fn build(&self) -> FuseQueryResult<Pipeline> {
        let mut pipeline = Pipeline::create();
        walk_postorder(&self.plan, |plan| match plan {
            PlanNode::Stage(_) => {
                pipeline.merge_processor()?;
                Ok(true)
            }
            PlanNode::Projection(plan) => {
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(ProjectionTransform::try_create(
                        self.ctx.clone(),
                        plan.schema.clone(),
                        plan.expr.clone(),
                    )?))
                })?;
                Ok(true)
            }
            PlanNode::AggregatorPartial(plan) => {
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(AggregatorPartialTransform::try_create(
                        self.ctx.clone(),
                        plan.schema(),
                        plan.aggr_expr.clone(),
                    )?))
                })?;
                Ok(true)
            }
            PlanNode::AggregatorFinal(plan) => {
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(AggregatorFinalTransform::try_create(
                        self.ctx.clone(),
                        plan.schema(),
                        plan.aggr_expr.clone(),
                    )?))
                })?;
                Ok(true)
            }
            PlanNode::Filter(plan) => {
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(FilterTransform::try_create(
                        self.ctx.clone(),
                        plan.predicate.clone(),
                    )?))
                })?;
                Ok(true)
            }
            PlanNode::Limit(plan) => {
                pipeline
                    .add_simple_transform(|| Ok(Box::new(LimitTransform::try_create(plan.n)?)))?;
                if pipeline.pipe_num() > 1 {
                    pipeline.merge_processor()?;
                    pipeline.add_simple_transform(|| {
                        Ok(Box::new(LimitTransform::try_create(plan.n)?))
                    })?;
                }
                Ok(false)
            }
            PlanNode::Sort(plan) => {
                let limit = PipelineBuilder::get_limit(&self.plan);
                pipeline.add_simple_transform(|| {
                    Ok(Box::new(PartialSortTransform::try_create(
                        self.ctx.clone(),
                        plan.schema(),
                        plan.order_by.clone(),
                        limit,
                    )?))
                })?;

                pipeline.add_simple_transform(|| {
                    Ok(Box::new(MergingSortTransform::try_create(
                        self.ctx.clone(),
                        plan.schema(),
                        plan.order_by.clone(),
                        limit,
                    )?))
                })?;

                // TODO merge_sort_stage
                if pipeline.pipe_num() > 1 {
                    let merge_sorted_processor = MergingSortedProcessor::try_create(
                        self.ctx.clone(),
                        plan.schema(),
                        plan.order_by.clone(),
                        limit,
                    )?;
                    pipeline.merge_sort_processor(merge_sorted_processor)?;
                }
                Ok(true)
            }
            PlanNode::ReadSource(plan) => {
                // Bind plan partitions to context.
                self.ctx.try_set_partitions(plan.partitions.clone())?;

                let max_threads = self.ctx.get_max_threads()? as usize;
                let workers = if max_threads == 0 {
                    1
                } else if max_threads > plan.partitions.len() {
                    plan.partitions.len()
                } else {
                    max_threads
                };

                for _i in 0..workers {
                    let source = SourceTransform::try_create(
                        self.ctx.clone(),
                        plan.db.as_str(),
                        plan.table.as_str(),
                    )?;
                    pipeline.add_source(Arc::new(source))?;
                }
                Ok(true)
            }
            other => {
                return Err(FuseQueryError::Internal(format!(
                    "Build pipeline from the plan node unsupported:{:?}",
                    other.name()
                )));
            }
        })?;

        pipeline.merge_processor()?;
        Ok(pipeline)
    }


    pub fn get_limit(plan: &PlanNode) -> Option<usize> {
        let mut result = None;

        let _ = walk_postorder(plan, |p| match p {
            PlanNode::Limit(limit) => {
                result = Some(limit.n);
                Ok(false)
            }
            _ => Ok(true),
        });

        result
    }
}
