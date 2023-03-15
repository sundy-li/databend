// Copyright 2021 Datafuse Labs.
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

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use common_base::base::convert_byte_size;
use common_base::base::convert_number_size;
use common_base::base::tokio::io::AsyncWrite;
use common_base::runtime::TrySpawn;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SendableDataBlockStream;
use common_sql::plans::Plan;
use common_sql::Planner;
use common_users::CertifiedInfo;
use common_users::UserApiProvider;
use futures_util::StreamExt;
use metrics::histogram;
use opensrv_mysql::AsyncMysqlShim;
use opensrv_mysql::ErrorKind;
use opensrv_mysql::InitWriter;
use opensrv_mysql::ParamParser;
use opensrv_mysql::QueryResultWriter;
use opensrv_mysql::StatementMetaWriter;
use rand::RngCore;
use tracing::error;
use tracing::info;
use tracing::Instrument;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterQueryLog;
use crate::servers::mysql::writers::DFInitResultWriter;
use crate::servers::mysql::writers::DFQueryResultWriter;
use crate::servers::mysql::writers::ProgressReporter;
use crate::servers::mysql::writers::QueryResult;
use crate::servers::mysql::MySQLFederated;
use crate::servers::mysql::MYSQL_VERSION;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::TableContext;
use crate::stream::DataBlockStream;

fn has_result_set_by_plan(plan: &Plan) -> bool {
    matches!(
        plan,
        Plan::Query { .. }
            | Plan::Explain { .. }
            | Plan::ExplainAst { .. }
            | Plan::ExplainSyntax { .. }
            | Plan::ExplainAnalyze { .. }
            | Plan::Call(_)
            | Plan::ShowCreateDatabase(_)
            | Plan::ShowCreateTable(_)
            | Plan::ShowFileFormats(_)
            | Plan::ShowRoles(_)
            | Plan::DescShare(_)
            | Plan::ShowShares(_)
            | Plan::ShowObjectGrantPrivileges(_)
            | Plan::ShowGrantTenantsOfShare(_)
            | Plan::DescribeTable(_)
            | Plan::ShowGrants(_)
            | Plan::ListStage(_)
            | Plan::Presign(_)
    )
}

struct InteractiveWorkerBase<W: AsyncWrite + Send + Unpin> {
    session: Arc<Session>,
    generic_hold: PhantomData<W>,
}

pub struct InteractiveWorker<W: AsyncWrite + Send + Unpin> {
    base: InteractiveWorkerBase<W>,
    version: String,
    salt: [u8; 20],
    client_addr: String,
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Sync + Unpin> AsyncMysqlShim<W> for InteractiveWorker<W> {
    type Error = ErrorCode;

    fn version(&self) -> &str {
        self.version.as_str()
    }

    fn connect_id(&self) -> u32 {
        match self.base.session.get_mysql_conn_id() {
            Some(conn_id) => conn_id,
            None => {
                // default conn id
                u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
            }
        }
    }

    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        "mysql_native_password"
    }

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let username = String::from_utf8_lossy(username);
        let client_addr = self.client_addr.clone();
        let info = CertifiedInfo::create(&username, auth_data, &client_addr);

        let authenticate = self.base.authenticate(salt, info);
        match authenticate.await {
            Ok(res) => res,
            Err(failure) => {
                error!(
                    "MySQL handler authenticate failed, \
                        user_name: {}, \
                        client_address: {}, \
                        failure_cause: {}",
                    username, client_addr, failure
                );
                false
            }
        }
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        writer: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_prepare(query, writer).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        param: ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        self.base.do_execute(id, param, writer).await
    }

    /// https://dev.mysql.com/doc/internals/en/com-stmt-close.html
    async fn on_close<'a>(&'a mut self, stmt_id: u32)
    where W: 'async_trait {
        self.base.do_close(stmt_id).await;
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        let mut writer = DFQueryResultWriter::create(writer);

        let instant = Instant::now();
        let query_result = self
            .base
            .do_query(query)
            .await
            .map_err(|err| err.display_with_sql(query));

        let format = self.base.session.get_format_settings()?;
        let mut write_result = writer.write(query_result, &format).await;

        if let Err(cause) = write_result {
            let suffix = format!("(while in query {})", query);
            write_result = Err(cause.add_message_back(suffix));
        }

        histogram!(
            super::mysql_metrics::METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
            instant.elapsed()
        );

        write_result
    }

    async fn on_init<'a>(
        &'a mut self,
        database_name: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<()> {
        if self.base.session.is_aborting() {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    "Aborting this connection. because we are try aborting server.".as_bytes(),
                )
                .await?;

            return Err(ErrorCode::AbortedSession(
                "Aborting this connection. because we are try aborting server.",
            ));
        }

        DFInitResultWriter::create(writer)
            .write(self.base.do_init(database_name).await)
            .await
    }
}

impl<W: AsyncWrite + Send + Unpin> InteractiveWorkerBase<W> {
    async fn authenticate(&self, salt: &[u8], info: CertifiedInfo) -> Result<bool> {
        let user_name = &info.user_name;
        let client_ip = info.user_client_address.split(':').collect::<Vec<_>>()[0];

        let ctx = self.session.create_query_context().await?;
        let user_info = UserApiProvider::instance()
            .get_user_with_client_ip(&ctx.get_tenant(), user_name, client_ip)
            .await?;

        let authed = user_info.auth_info.auth_mysql(&info.user_password, salt)?;
        if authed {
            self.session.set_authed_user(user_info, None).await?;
        }
        Ok(authed)
    }

    async fn do_prepare(&mut self, _: &str, writer: StatementMetaWriter<'_, W>) -> Result<()> {
        writer
            .error(
                ErrorKind::ER_UNKNOWN_ERROR,
                "Prepare is not support in Databend.".as_bytes(),
            )
            .await?;
        Ok(())
    }

    async fn do_execute(
        &mut self,
        _: u32,
        _: ParamParser<'_>,
        writer: QueryResultWriter<'_, W>,
    ) -> Result<()> {
        writer
            .error(
                ErrorKind::ER_UNKNOWN_ERROR,
                "Execute is not support in Databend.".as_bytes(),
            )
            .await?;
        Ok(())
    }

    async fn do_close(&mut self, _: u32) {}

    // Check the query is a federated or driver setup command.
    // Here we fake some values for the command which Databend not supported.
    fn federated_server_command_check(&self, query: &str) -> Option<(DataSchemaRef, DataBlock)> {
        // INSERT don't need MySQL federated check
        if query.len() > 6 && query[..6].eq_ignore_ascii_case("INSERT") {
            return None;
        }
        let federated = MySQLFederated::create();
        federated.check(query)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn do_query(&mut self, query: &str) -> Result<QueryResult> {
        match self.federated_server_command_check(query) {
            Some((schema, data_block)) => {
                info!("Federated query: {}", query);
                if data_block.num_rows() > 0 {
                    info!("Federated response: {:?}", data_block);
                }
                let has_result = data_block.num_rows() > 0;
                Ok(QueryResult::create(
                    DataBlockStream::create(None, vec![data_block]).boxed(),
                    None,
                    has_result,
                    schema,
                ))
            }
            None => {
                info!("Normal query: {}", query);
                let context = self.session.create_query_context().await?;

                let mut planner = Planner::new(context.clone());
                let (plan, extras) = planner.plan_sql(query).await?;

                context.attach_query_str(plan.to_string(), extras.stament.to_mask_sql());
                let interpreter = InterpreterFactory::get(context.clone(), &plan).await;
                let has_result_set = has_result_set_by_plan(&plan);

                match interpreter {
                    Ok(interpreter) => {
                        let (blocks, extra_info) =
                            Self::exec_query(interpreter.clone(), &context).await?;
                        let schema = interpreter.schema();
                        Ok(QueryResult::create(
                            blocks,
                            extra_info,
                            has_result_set,
                            schema,
                        ))
                    }
                    Err(e) => {
                        InterpreterQueryLog::fail_to_start(context, e.clone());
                        Err(e)
                    }
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(interpreter, context))]
    async fn exec_query(
        interpreter: Arc<dyn Interpreter>,
        context: &Arc<QueryContext>,
    ) -> Result<(
        SendableDataBlockStream,
        Option<Box<dyn ProgressReporter + Send>>,
    )> {
        let instant = Instant::now();

        let query_result = context.try_spawn({
            let ctx = context.clone();
            async move {
                let mut data_stream = interpreter.execute(ctx.clone()).await?;
                histogram!(
                    super::mysql_metrics::METRIC_INTERPRETER_USEDTIME,
                    instant.elapsed()
                );

                // Wrap the data stream, log finish event at the end of stream
                let intercepted_stream = async_stream::stream! {

                    while let Some(item) = data_stream.next().await {
                        yield item
                    };
                };

                Ok::<_, ErrorCode>(intercepted_stream.boxed())
            }
            .in_current_span()
        })?;

        let query_result = query_result.await.map_err_to_code(
            ErrorCode::TokioError,
            || "Cannot join handle from context's runtime",
        )?;
        let reporter = Box::new(ContextProgressReporter::new(context.clone(), instant))
            as Box<dyn ProgressReporter + Send>;
        query_result.map(|data| (data, Some(reporter)))
    }

    async fn do_init(&mut self, database_name: &str) -> Result<()> {
        if database_name.is_empty() {
            return Ok(());
        }
        let init_query = format!("USE `{}`;", database_name);

        let do_query = self.do_query(&init_query).await;
        match do_query {
            Ok(_) => Ok(()),
            Err(error_code) => Err(error_code),
        }
    }
}

impl<W: AsyncWrite + Send + Unpin> InteractiveWorker<W> {
    pub fn create(session: Arc<Session>, client_addr: String) -> InteractiveWorker<W> {
        let mut bs = vec![0u8; 20];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(bs.as_mut());

        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i] & 0x7fu8;
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }

        InteractiveWorker::<W> {
            base: InteractiveWorkerBase::<W> {
                session,
                generic_hold: PhantomData::default(),
            },
            salt: scramble,
            version: format!("{}-{}", MYSQL_VERSION, *DATABEND_COMMIT_VERSION),
            client_addr,
        }
    }
}

struct ContextProgressReporter {
    context: Arc<QueryContext>,
    instant: Instant,
}

impl ContextProgressReporter {
    fn new(context: Arc<QueryContext>, instant: Instant) -> Self {
        Self { context, instant }
    }
}

impl ProgressReporter for ContextProgressReporter {
    fn progress_info(&self) -> String {
        let progress = self.context.get_scan_progress_value();
        let seconds = self.instant.elapsed().as_nanos() as f64 / 1e9f64;
        format!(
            "Read {} rows, {} in {:.3} sec., {} rows/sec., {}/sec.",
            progress.rows,
            convert_byte_size(progress.bytes as f64),
            seconds,
            convert_number_size((progress.rows as f64) / (seconds)),
            convert_byte_size((progress.bytes as f64) / (seconds)),
        )
    }

    fn affected_rows(&self) -> u64 {
        let progress = self.context.get_scan_progress_value();
        progress.rows as u64
    }
}
