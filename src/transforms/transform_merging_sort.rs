// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;

use crate::datablocks::data_block_sort::merge_sort_blocks;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};
use crate::sessions::FuseQueryContextRef;
use crate::transforms::get_sort_descriptions;
use futures::StreamExt;

pub struct MergingSortTransform {
    ctx: FuseQueryContextRef,
    schema: DataSchemaRef,
    exprs: Vec<ExpressionPlan>,
    limit: Option<usize>,
    input: Arc<dyn IProcessor>,
}

impl MergingSortTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        exprs: Vec<ExpressionPlan>,
        limit: Option<usize>,
    ) -> FuseQueryResult<Self> {
        Ok(MergingSortTransform {
            ctx,
            schema,
            exprs,
            limit,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for MergingSortTransform {
    fn name(&self) -> &str {
        "MergingSortTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let sort_columns_descriptions =
            get_sort_descriptions(self.ctx.clone(), &self.schema, &self.exprs)?;
        let mut stream = self.input.execute().await?;
        let mut num = 0;
        let mut merge_block = None;

        while let Some(block) = stream.next().await {
            num = num + 1;

            let block = block?;
            if num == 1 {
                merge_block = Some(block);
            } else {
                merge_block = Some(merge_sort_blocks(
                    &[merge_block.unwrap(), block],
                    &sort_columns_descriptions,
                    self.limit,
                )?);
            }
        }

        let results = match merge_block {
            None => vec![],
            Some(block) => vec![block],
        };

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            results,
        )))
    }
}
