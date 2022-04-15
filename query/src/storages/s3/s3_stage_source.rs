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

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_io::prelude::S3File;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageStorage;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::S3StageTableInfo;
use common_streams::CsvSourceBuilder;
use common_streams::NDJsonSourceBuilder;
use common_streams::ParquetSourceBuilder;
use common_streams::Source;
use futures::io::BufReader;
use opendal::io_util::SeekableReader;
use opendal::BytesReader;
use opendal::Operator;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSource;
use crate::pipelines::new::processors::AsyncSourcer;
use crate::sessions::QueryContext;

pub struct StageSource {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    table_info: S3StageTableInfo,
    initialized: bool,
    source: Option<Box<dyn Source>>,
    files: Arc<Mutex<VecDeque<String>>>,
}

impl StageSource {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        table_info: S3StageTableInfo,
        files: Arc<Mutex<VecDeque<String>>>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, StageSource {
            ctx,
            schema,
            table_info,
            initialized: false,
            source: None,
            files,
        })
    }

    // Get csv source stream.
    async fn csv_source(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        stage_info: &UserStageInfo,
        reader: BytesReader,
    ) -> Result<Box<dyn Source>> {
        let settings = ctx.get_format_settings()?;
        let mut builder = CsvSourceBuilder::create(schema, settings);
        let size_limit = stage_info.copy_options.size_limit;

        // Size limit.
        {
            if size_limit > 0 {
                builder.size_limit(size_limit);
            }
        }

        // Block size.
        {
            let max_block_size = ctx.get_settings().get_max_block_size()?;
            builder.block_size(max_block_size as usize);
        }

        // Skip header.
        {
            builder.skip_header(stage_info.file_format_options.skip_header > 0);
        }

        // Field delimiter, default ','.
        {
            let field_delimiter = &stage_info.file_format_options.field_delimiter;
            builder.field_delimiter(field_delimiter);
        }

        // Record delimiter, default '\n'.
        {
            let record_delimiter = &stage_info.file_format_options.record_delimiter;
            builder.record_delimiter(record_delimiter);
        }

        Ok(Box::new(builder.build(reader)?))
    }

    // Get json source stream.
    async fn json_source(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        stage_info: &UserStageInfo,
        reader: BytesReader,
    ) -> Result<Box<dyn Source>> {
        let mut builder = NDJsonSourceBuilder::create(schema);
        let size_limit = stage_info.copy_options.size_limit;

        // Size limit.
        {
            if size_limit > 0 {
                builder.size_limit(size_limit);
            }
        }

        // Block size.
        {
            let max_block_size = ctx.get_settings().get_max_block_size()?;
            builder.block_size(max_block_size as usize);
        }

        Ok(Box::new(builder.build(BufReader::new(reader))?))
    }

    // Get parquet source stream.
    async fn parquet_source(
        _ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        _stage_info: &UserStageInfo,
        reader: SeekableReader,
    ) -> Result<Box<dyn Source>> {
        let mut builder = ParquetSourceBuilder::create(schema.clone());

        // Default is all the columns.
        let default_proj = (0..schema.fields().len())
            .into_iter()
            .collect::<Vec<usize>>();
        builder.projection(default_proj);

        Ok(Box::new(builder.build(reader)?))
    }

    pub async fn get_op(ctx: &Arc<QueryContext>, stage: &UserStageInfo) -> Result<Operator> {
        if stage.stage_type == StageType::Internal {
            ctx.get_storage_operator()
        } else {
            // Get the dal file reader.
            match &stage.stage_params.storage {
                StageStorage::S3(s3) => {
                    let endpoint = &ctx.get_config().storage.s3.endpoint_url;
                    let bucket = &s3.bucket;

                    let key_id = &s3.credentials_aws_key_id;
                    let secret_key = &s3.credentials_aws_secret_key;

                    S3File::open(endpoint, bucket, key_id, secret_key, "/").await
                }
            }
        }
    }

    async fn initialize(&mut self, file_name: String) -> Result<()> {
        let ctx = self.ctx.clone();
        let stage = &self.table_info.stage_info;
        let file_format = stage.file_format_options.format.clone();

        let op = Self::get_op(&self.ctx, &self.table_info.stage_info).await?;
        let path = file_name;
        let object = op.object(&path);

        // Get the format(CSV, Parquet) source stream.
        let source = match &file_format {
            StageFileFormatType::Csv => Ok(Self::csv_source(
                ctx.clone(),
                self.schema.clone(),
                stage,
                Box::new(object.reader().await?),
            )
            .await?),
            StageFileFormatType::Json => Ok(Self::json_source(
                ctx.clone(),
                self.schema.clone(),
                stage,
                Box::new(object.reader().await?),
            )
            .await?),
            StageFileFormatType::Parquet => Ok(Self::parquet_source(
                ctx.clone(),
                self.schema.clone(),
                stage,
                object.seekable_reader(..),
            )
            .await?),
            // Unsupported.
            format => Err(ErrorCode::LogicalError(format!(
                "Unsupported file format: {:?}",
                format
            ))),
        }?;
        self.source = Some(source);

        Ok(())
    }
}

impl AsyncSource for StageSource {
    const NAME: &'static str = "StageSource";

    type BlockFuture<'a> = impl Future<Output = Result<Option<DataBlock>>> where Self: 'a;

    fn generate(&mut self) -> Self::BlockFuture<'_> {
        let mut files_guard = self.files.lock();
        let file_name = files_guard.pop_front();
        drop(files_guard);
        async move {
            if !self.initialized {
                self.initialized = true;
            }

            if file_name.is_none() {
                return Ok(None);
            }
            self.initialize(file_name.unwrap()).await?;

            match &mut self.source {
                None => Err(ErrorCode::LogicalError("Please init source first!")),
                Some(source) => match source.read().await? {
                    None => Ok(None),
                    Some(data) => Ok(Some(data)),
                },
            }
        }
    }
}
