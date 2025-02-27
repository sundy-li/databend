// Copyright 2021 Datafuse Labs
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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use databend_common_base::headers::HEADER_FUNCTION;
use databend_common_base::headers::HEADER_FUNCTION_HANDLER;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_base::http_client::GLOBAL_HTTP_CLIENT;
use databend_common_base::version::DATABEND_SEMVER;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::DNSService;
use futures::stream;
use futures::StreamExt;
use futures::TryStreamExt;
use hyper_util::client::legacy::connect::HttpConnector;
use serde::Deserialize;
use serde::Serialize;
use tonic::metadata::KeyAndValueRef;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;
use tonic::transport::channel::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::Request;

use crate::types::DataType;
use crate::DataSchema;

const UDF_TCP_KEEP_ALIVE_SEC: u64 = 30;
const UDF_HTTP2_KEEP_ALIVE_INTERVAL_SEC: u64 = 60;
const UDF_KEEP_ALIVE_TIMEOUT_SEC: u64 = 20;
// 4MB by default, we use 16G
// max_encoding_message_size is usize::max by default
const MAX_DECODING_MESSAGE_SIZE: usize = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct UDFFlightClient {
    inner: FlightServiceClient<Channel>,
    batch_rows: usize,
    headers: MetadataMap,
}

pub enum UDFClient {
    Flight(UDFFlightClient),
    Http(MetadataMap, String),
}

impl UDFClient {
    pub async fn is_http(addr: &str) -> bool {
        let client = GLOBAL_HTTP_CLIENT.inner();
        if let Ok(resp) = client.get(addr).send().await {
            if resp.status().as_u16() != 200 {
                return false;
            }
            #[derive(Deserialize, Serialize)]
            struct Resp {
                protocol: String,
            }
            return resp
                .json::<Resp>()
                .await
                .map(|c| c.protocol == "http")
                .unwrap_or_default();
        }
        false
    }

    #[async_backtrace::framed]
    pub async fn connect(
        endpoint: Arc<Endpoint>,
        conn_timeout: u64,
        batch_rows: usize,
    ) -> Result<Self> {
        let uri = endpoint.uri();
        if Self::is_http(&uri.to_string()).await {
            Ok(Self::Http(MetadataMap::default(), uri.to_string()))
        } else {
            let c = UDFFlightClient::connect(endpoint, conn_timeout, batch_rows).await?;
            Ok(Self::Flight(c))
        }
    }

    #[async_backtrace::framed]
    pub async fn check_schema(
        &mut self,
        func_name: &str,
        arg_types: &[DataType],
        return_type: &DataType,
    ) -> Result<()> {
        let schema = match self {
            UDFClient::Flight(c) => c.get_udf_schema(func_name).await?,
            UDFClient::Http(headers, addr) => {
                let client = GLOBAL_HTTP_CLIENT.inner();
                let post_url = format!("{}/{}", addr.trim_end_matches('/'), func_name);
                let resp = client
                    .get(post_url.as_str())
                    .headers(headers.clone().into_headers())
                    .send()
                    .await?;
                let bytes = resp.bytes().await?;
                let reader = StreamReader::try_new(bytes.as_ref(), None)?;
                reader.schema().as_ref().clone()
            }
        };
        let schema = DataSchema::try_from(&schema)?;
        let fields_num = schema.fields().len();
        if fields_num == 0 {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF Server should return at least one column on UDF function {func_name}"
            )));
        }

        let (input_fields, output_fields) = schema.fields().split_at(fields_num - 1);
        let remote_arg_types = input_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let expect_return_type = output_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        if remote_arg_types != arg_types {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF arg types mismatch on UDF function {}, remote arg types: ({:?}), defined arg types: ({:?})",
                func_name,
                remote_arg_types
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                arg_types
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        if &expect_return_type[0] != return_type {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF return type mismatch on UDF function {}, expected return type: {}, actual return type: {}",
                func_name,
                expect_return_type[0],
                return_type
            )));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn execute_udf(
        &mut self,
        func_name: &str,
        input_batch: RecordBatch,
    ) -> Result<RecordBatch> {
        match self {
            UDFClient::Flight(c) => c.execute_udf(func_name, input_batch).await,
            UDFClient::Http(headers, addr) => {
                let client = GLOBAL_HTTP_CLIENT.inner();
                let post_url = format!("{}/{}", addr.trim_end_matches('/'), func_name);

                // Encode input_batch into IPC format
                let mut body = Vec::new();
                let mut writer =
                    arrow_ipc::writer::StreamWriter::try_new(&mut body, &input_batch.schema())?;
                writer.write(&input_batch)?;
                writer.finish()?;

                let resp = client
                    .post(post_url.as_str())
                    .headers(headers.clone().into_headers())
                    .body(body)
                    .send()
                    .await?;
                let bytes = resp.bytes().await?;
                let mut reader = StreamReader::try_new(bytes.as_ref(), None)?;
                Ok(reader
                    .next()
                    .ok_or_else(|| ErrorCode::Internal("expected one arrow array"))??)
            }
        }
    }

    pub fn with_headers<'a, H: IntoIterator<Item = (&'a str, &'a str)>>(
        mut self,
        headers: H,
    ) -> Result<Self> {
        for (key, value) in headers.into_iter() {
            let key = MetadataKey::from_str(key)
                .map_err(|err| ErrorCode::UDFDataError(format!("Parse key {key} error: {err}")))?;
            let value = MetadataValue::from_str(value).map_err(|err| {
                ErrorCode::UDFDataError(format!("Parse value {value} error: {err}"))
            })?;
            match self {
                UDFClient::Flight(ref mut c) => c.headers.insert(key, value),
                UDFClient::Http(ref mut c, _) => c.insert(key, value),
            };
        }
        Ok(self)
    }

    /// Set tenant for the UDF client.
    pub fn with_tenant(self, tenant: &str) -> Result<Self> {
        self.with_headers([(HEADER_TENANT, tenant)])
    }

    /// Set function name for the UDF client.
    pub fn with_func_name(self, func_name: &str) -> Result<Self> {
        self.with_headers([(HEADER_FUNCTION, func_name)])
    }

    pub fn with_handler_name(self, handler_name: &str) -> Result<Self> {
        self.with_headers([(HEADER_FUNCTION_HANDLER, handler_name)])
    }

    /// Set query id for the UDF client.
    pub fn with_query_id(self, query_id: &str) -> Result<Self> {
        self.with_headers([(HEADER_QUERY_ID, query_id)])
    }
}

// Flight based client
impl UDFFlightClient {
    pub fn build_endpoint(
        addr: &str,
        conn_timeout: u64,
        request_timeout: u64,
    ) -> Result<Arc<Endpoint>> {
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let endpoint = Endpoint::from_shared(addr.to_string())
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!("Invalid UDF Server address: {err}"))
            })?
            .user_agent(format!("databend-query/{}", *DATABEND_SEMVER))
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!("Invalid UDF Client User Agent: {err}"))
            })?
            .connect_timeout(Duration::from_secs(conn_timeout))
            .timeout(Duration::from_secs(request_timeout))
            .tcp_keepalive(Some(Duration::from_secs(UDF_TCP_KEEP_ALIVE_SEC)))
            .http2_keep_alive_interval(Duration::from_secs(UDF_HTTP2_KEEP_ALIVE_INTERVAL_SEC))
            .keep_alive_timeout(Duration::from_secs(UDF_KEEP_ALIVE_TIMEOUT_SEC))
            .keep_alive_while_idle(true)
            .tls_config(tls_config)
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!("Invalid UDF Client TLS Config: {err}"))
            })?;

        Ok(Arc::new(endpoint))
    }

    #[async_backtrace::framed]
    pub async fn connect(
        endpoint: Arc<Endpoint>,
        conn_timeout: u64,
        batch_rows: usize,
    ) -> Result<UDFFlightClient> {
        let mut connector = HttpConnector::new_with_resolver(DNSService);
        connector.enforce_http(false);
        connector.set_nodelay(true);
        // connector.set_keepalive(Some(Duration::from_secs(UDF_TCP_KEEP_ALIVE_SEC)));
        connector.set_connect_timeout(Some(Duration::from_secs(conn_timeout)));
        connector.set_reuse_address(true);

        let channel = endpoint
            .connect_with_connector(connector)
            .await
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!(
                    "Cannot connect to UDF Server {}: {:?}",
                    endpoint.uri(),
                    err
                ))
            })?;
        let inner =
            FlightServiceClient::new(channel).max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);
        Ok(UDFFlightClient {
            inner,
            batch_rows,
            headers: MetadataMap::new(),
        })
    }

    fn make_request<T>(&self, t: T) -> Request<T> {
        let mut request = Request::new(t);
        for k_v in self.headers.iter() {
            match k_v {
                KeyAndValueRef::Ascii(key, value) => {
                    request.metadata_mut().insert(key, value.clone());
                }
                KeyAndValueRef::Binary(key, value) => {
                    request.metadata_mut().insert_bin(key, value.clone());
                }
            }
        }

        request
    }

    #[async_backtrace::framed]
    pub async fn get_udf_schema(&mut self, func_name: &str) -> Result<Schema> {
        let descriptor = FlightDescriptor::new_path(vec![func_name.to_string()]);
        let request = self.make_request(descriptor);
        let flight_info = self.inner.get_flight_info(request).await?.into_inner();
        flight_info.try_decode_schema().map_err(|err| {
            ErrorCode::UDFDataError(format!(
                "Decode UDF schema failed on UDF function {func_name}: {err}"
            ))
        })
    }

    #[async_backtrace::framed]
    pub async fn execute_udf(
        &mut self,
        func_name: &str,
        input_batch: RecordBatch,
    ) -> Result<RecordBatch> {
        let descriptor = FlightDescriptor::new_path(vec![func_name.to_string()]);
        let batch_rows = self.batch_rows;
        let batches = (0..input_batch.num_rows())
            .step_by(batch_rows)
            .map(move |start| {
                Ok(input_batch.slice(start, batch_rows.min(input_batch.num_rows() - start)))
            });

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(stream::iter(batches))
            .map(|data| data.unwrap());
        let request = self.make_request(flight_data_stream);
        let flight_data_stream = self.inner.do_exchange(request).await?.into_inner();
        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            flight_data_stream.map_err(|err| err.into()),
        )
        .map_err(|err| {
            ErrorCode::UDFDataError(format!(
                "Decode record batch failed on UDF function {func_name}: {err}"
            ))
        });

        let batches: Vec<RecordBatch> = record_batch_stream.try_collect().await?;
        if batches.is_empty() {
            return Err(ErrorCode::EmptyDataFromServer(format!(
                "Get empty data from UDF Server on UDF function {func_name}"
            )));
        }

        let schema = batches[0].schema();
        concat_batches(&schema, batches.iter())
            .map_err(|err| ErrorCode::UDFDataError(err.to_string()))
    }
}

pub fn error_kind(message: &str) -> &str {
    let message = message.to_ascii_lowercase();
    if message.contains("timeout") || message.contains("timedout") {
        // Error(Connect, Custom)
        if message.contains("connect,") {
            "ConnectTimeout"
        } else {
            "RequestTimeout"
        }
    } else if message.contains("cannot connect") {
        "ConnectError"
    } else if message.contains("stream closed because of a broken pipe") {
        "ServerClosed"
    } else if message.contains("dns error") || message.contains("lookup address") {
        "DnsError"
    } else {
        "Other"
    }
}
