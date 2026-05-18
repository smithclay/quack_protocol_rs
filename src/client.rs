use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};

use crate::builders::{ColumnDefinition, data_chunk_from_rows};
use crate::constants::{DEFAULT_QUACK_PORT, DUCKDB_MIME_TYPE, QUACK_ENDPOINT, QUACK_VERSION};
use crate::errors::{QuackError, Result};
use crate::json::{JsonOptions, to_json_rows};
use crate::messages::{MessageHeader, MessageType, QuackMessage, decode_message, encode_message};
use crate::sql::{SqlParameters, format_sql};
use crate::vector::{DataChunk, Row, Value, chunks_to_rows};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedQuackUri {
    pub base_url: String,
    pub host: String,
    pub port: u16,
    pub ssl: bool,
}

#[derive(Clone, Debug, Default)]
pub struct QuackClientOptions {
    pub auth_token: Option<String>,
    pub client_duckdb_version: Option<String>,
    pub client_platform: Option<String>,
    pub min_supported_quack_version: Option<u64>,
    pub max_supported_quack_version: Option<u64>,
    pub ssl: Option<bool>,
    pub timeout: Option<Duration>,
    pub headers: HeaderMap,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuackConnectionInfo {
    pub server_duckdb_version: Option<String>,
    pub server_platform: Option<String>,
    pub quack_version: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QuackQueryResult {
    pub names: Vec<String>,
    pub types: Vec<crate::logical_types::LogicalType>,
    pub chunks: Vec<DataChunk>,
}

impl QuackQueryResult {
    pub fn rows(&self) -> Result<Vec<Row>> {
        chunks_to_rows(&self.chunks, Some(&self.names))
    }

    pub fn values(&self) -> Result<Vec<Value>> {
        let first_name = match self.names.first() {
            Some(name) => name,
            None => return Ok(Vec::new()),
        };
        Ok(self
            .rows()?
            .into_iter()
            .map(|mut row| row.shift_remove(first_name).unwrap_or(Value::Null))
            .collect())
    }

    pub fn json_rows(&self, options: JsonOptions) -> Result<Vec<serde_json::Value>> {
        to_json_rows(&self.rows()?, options)
    }
}

#[derive(Clone, Debug)]
pub struct QuackClient {
    pub base_url: String,
    pub info: Option<QuackConnectionInfo>,
    http: reqwest::Client,
    headers: HeaderMap,
    timeout: Option<Duration>,
    connection_id: Option<String>,
    closed: bool,
    next_query_id: u64,
}

impl QuackClient {
    pub async fn connect(uri: &str, options: QuackClientOptions) -> Result<Self> {
        let parsed = parse_quack_uri(uri, options.ssl)?;
        let mut client = Self {
            base_url: parsed.base_url.trim_end_matches('/').to_string(),
            info: None,
            http: reqwest::Client::new(),
            headers: options.headers.clone(),
            timeout: options.timeout,
            connection_id: None,
            closed: false,
            next_query_id: 1,
        };
        let response = client
            .send(&QuackMessage::ConnectionRequest {
                header: MessageHeader::new(MessageType::ConnectionRequest),
                auth_string: options.auth_token,
                client_duckdb_version: options.client_duckdb_version,
                client_platform: Some(
                    options
                        .client_platform
                        .unwrap_or_else(|| "quack-rust".to_string()),
                ),
                min_supported_quack_version: options
                    .min_supported_quack_version
                    .unwrap_or(QUACK_VERSION),
                max_supported_quack_version: options
                    .max_supported_quack_version
                    .unwrap_or(QUACK_VERSION),
            })
            .await?;

        match response {
            QuackMessage::ConnectionResponse {
                header,
                server_duckdb_version,
                server_platform,
                quack_version,
            } => {
                let connection_id = header.connection_id.ok_or_else(|| {
                    QuackError::protocol("CONNECTION_RESPONSE did not include a connection id")
                })?;
                client.connection_id = Some(connection_id);
                client.info = Some(QuackConnectionInfo {
                    server_duckdb_version,
                    server_platform,
                    quack_version,
                });
                Ok(client)
            }
            other => Err(QuackError::protocol(format!(
                "expected CONNECTION_RESPONSE, got {:?}",
                other.message_type()
            ))),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection_id.is_some() && !self.closed
    }

    pub async fn query(&mut self, sql: &str) -> Result<QuackQueryResult> {
        self.query_with_params(sql, None).await
    }

    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: Option<&SqlParameters>,
    ) -> Result<QuackQueryResult> {
        let sql = format_sql(sql, params)?;
        let prepare = self.prepare(&sql).await?;
        let (result_types, result_names, mut needs_more_fetch, mut chunks, result_uuid) =
            match prepare {
                QuackMessage::PrepareResponse {
                    result_types,
                    result_names,
                    needs_more_fetch,
                    results,
                    result_uuid,
                    ..
                } => (
                    result_types,
                    result_names,
                    needs_more_fetch,
                    results,
                    result_uuid,
                ),
                other => {
                    return Err(QuackError::protocol(format!(
                        "expected PREPARE_RESPONSE, got {:?}",
                        other.message_type()
                    )));
                }
            };

        attach_column_names(&mut chunks, &result_names);
        while needs_more_fetch {
            let fetch = self.fetch_result(result_uuid).await?;
            match fetch {
                QuackMessage::FetchResponse { mut results, .. } => {
                    if results.is_empty() {
                        needs_more_fetch = false;
                    } else {
                        attach_column_names(&mut results, &result_names);
                        chunks.extend(results);
                    }
                }
                other => {
                    return Err(QuackError::protocol(format!(
                        "expected FETCH_RESPONSE, got {:?}",
                        other.message_type()
                    )));
                }
            }
        }
        Ok(QuackQueryResult {
            names: result_names,
            types: result_types,
            chunks,
        })
    }

    pub async fn first(&mut self, sql: &str) -> Result<Option<Row>> {
        Ok(self.query(sql).await?.rows()?.into_iter().next())
    }

    pub async fn one(&mut self, sql: &str) -> Result<Row> {
        let rows = self.query(sql).await?.rows()?;
        if rows.len() != 1 {
            return Err(QuackError::protocol(format!(
                "expected exactly one row, got {}",
                rows.len()
            )));
        }
        Ok(rows.into_iter().next().expect("one row"))
    }

    pub async fn values(&mut self, sql: &str) -> Result<Vec<Value>> {
        self.query(sql).await?.values()
    }

    pub async fn append(
        &mut self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        chunk: DataChunk,
    ) -> Result<()> {
        self.ensure_open()?;
        let message = QuackMessage::AppendRequest {
            header: self.scoped_header(MessageType::AppendRequest)?,
            schema_name,
            table_name: table_name.into(),
            append_chunk: chunk,
        };
        let response = self.send(&message).await?;
        expect_success(response)
    }

    pub async fn append_rows(
        &mut self,
        table_name: impl Into<String>,
        schema_name: Option<String>,
        rows: &[Row],
        columns: Option<Vec<ColumnDefinition>>,
        batch_size: Option<usize>,
    ) -> Result<()> {
        let table_name = table_name.into();
        if rows.is_empty() {
            let chunk = data_chunk_from_rows(rows, columns)?;
            return self.append(table_name, schema_name, chunk).await;
        }
        let batch_size = batch_size.unwrap_or(rows.len());
        if batch_size == 0 {
            return Err(QuackError::protocol(
                "append_rows batch_size must be at least 1",
            ));
        }
        for batch in rows.chunks(batch_size) {
            let chunk = data_chunk_from_rows(batch, columns.clone())?;
            self.append(table_name.clone(), schema_name.clone(), chunk)
                .await?;
        }
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        if self.closed || self.connection_id.is_none() {
            self.closed = true;
            return Ok(());
        }
        let message = QuackMessage::Disconnect {
            header: self.scoped_header(MessageType::DisconnectMessage)?,
        };
        let response = self.send(&message).await?;
        expect_success(response)?;
        self.closed = true;
        self.connection_id = None;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.disconnect().await
    }

    pub async fn send(&self, message: &QuackMessage) -> Result<QuackMessage> {
        let bytes = encode_message(message)?;
        let mut request = self
            .http
            .post(format!("{}{}", self.base_url, QUACK_ENDPOINT))
            .header(ACCEPT, HeaderValue::from_static(DUCKDB_MIME_TYPE))
            .header(CONTENT_TYPE, HeaderValue::from_static(DUCKDB_MIME_TYPE))
            .body(bytes);
        if !self.headers.is_empty() {
            request = request.headers(self.headers.clone());
        }
        if let Some(timeout) = self.timeout {
            request = request.timeout(timeout);
        }
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(QuackError::protocol(format!(
                "Quack HTTP request failed with {} {}",
                response.status().as_u16(),
                response.status().canonical_reason().unwrap_or("")
            )));
        }
        let bytes = response.bytes().await?;
        let decoded = decode_message(&bytes)?;
        if let QuackMessage::ErrorResponse { message, .. } = decoded {
            return Err(QuackError::server(message));
        }
        Ok(decoded)
    }

    async fn prepare(&mut self, sql: &str) -> Result<QuackMessage> {
        self.ensure_open()?;
        let message = QuackMessage::PrepareRequest {
            header: self.scoped_header(MessageType::PrepareRequest)?,
            sql: sql.to_string(),
        };
        self.send(&message).await
    }

    async fn fetch_result(
        &mut self,
        result_uuid: crate::binary::HugeIntParts,
    ) -> Result<QuackMessage> {
        self.ensure_open()?;
        let message = QuackMessage::FetchRequest {
            header: self.scoped_header(MessageType::FetchRequest)?,
            result_uuid,
        };
        self.send(&message).await
    }

    fn scoped_header(&mut self, message_type: MessageType) -> Result<MessageHeader> {
        let connection_id = self
            .connection_id
            .clone()
            .ok_or_else(|| QuackError::protocol("Quack client is not connected"))?;
        let query_id = self.next_query_id;
        self.next_query_id += 1;
        Ok(MessageHeader::new(message_type)
            .with_connection(connection_id)
            .with_client_query_id(query_id))
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed || self.connection_id.is_none() {
            Err(QuackError::protocol("Quack client is not connected"))
        } else {
            Ok(())
        }
    }
}

pub fn parse_quack_uri(input: &str, ssl_override: Option<bool>) -> Result<ParsedQuackUri> {
    let uri = input.trim();
    if uri.is_empty() {
        return Err(QuackError::protocol("Quack URI is empty"));
    }
    if uri.starts_with("http://") || uri.starts_with("https://") {
        let url = url::Url::parse(uri)?;
        let ssl = url.scheme() == "https";
        let port = url
            .port_or_known_default()
            .unwrap_or(if ssl { 443 } else { 80 });
        let host = url
            .host_str()
            .ok_or_else(|| QuackError::protocol(format!("invalid Quack URI host {input}")))?
            .to_string();
        let host_for_base = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.clone()
        };
        return Ok(ParsedQuackUri {
            base_url: format!("{}://{}:{port}", url.scheme(), host_for_base),
            host,
            port,
            ssl,
        });
    }

    let rest = uri
        .strip_prefix("quack://")
        .or_else(|| uri.strip_prefix("quack:"))
        .unwrap_or(uri);
    if rest.is_empty() {
        return Err(QuackError::protocol(format!("invalid Quack URI {input}")));
    }
    let (host, port) = parse_host_port(rest)?;
    let ssl = ssl_override.unwrap_or(false);
    let protocol = if ssl { "https" } else { "http" };
    let host_for_base = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    Ok(ParsedQuackUri {
        base_url: format!("{protocol}://{host_for_base}:{port}"),
        host,
        port,
        ssl,
    })
}

fn parse_host_port(value: &str) -> Result<(String, u16)> {
    if let Some(rest) = value.strip_prefix('[') {
        let end = rest
            .find(']')
            .ok_or_else(|| QuackError::protocol(format!("invalid IPv6 Quack URI host {value}")))?;
        let host = rest[..end].to_string();
        let suffix = &rest[end + 1..];
        let port = if let Some(port) = suffix.strip_prefix(':') {
            parse_port(port)?
        } else {
            DEFAULT_QUACK_PORT
        };
        return Ok((host, port));
    }
    let colon_count = value.chars().filter(|ch| *ch == ':').count();
    match colon_count {
        0 => Ok((value.to_string(), DEFAULT_QUACK_PORT)),
        1 => {
            let (host, port) = value
                .split_once(':')
                .ok_or_else(|| QuackError::protocol(format!("invalid Quack URI {value}")))?;
            if host.is_empty() {
                return Err(QuackError::protocol(format!(
                    "invalid Quack URI host {value}"
                )));
            }
            Ok((host.to_string(), parse_port(port)?))
        }
        _ => Err(QuackError::protocol(format!(
            "IPv6 Quack URI hosts must be enclosed in []: {value}"
        ))),
    }
}

fn parse_port(value: &str) -> Result<u16> {
    let port = value
        .parse::<u16>()
        .map_err(|_| QuackError::protocol(format!("invalid Quack URI port {value}")))?;
    if port == 0 {
        return Err(QuackError::protocol(format!(
            "invalid Quack URI port {value}"
        )));
    }
    Ok(port)
}

fn attach_column_names(chunks: &mut [DataChunk], names: &[String]) {
    for chunk in chunks {
        chunk.column_names = Some(names.to_vec());
    }
}

fn expect_success(response: QuackMessage) -> Result<()> {
    match response {
        QuackMessage::SuccessResponse { .. } => Ok(()),
        other => Err(QuackError::protocol(format!(
            "expected SUCCESS_RESPONSE, got {:?}",
            other.message_type()
        ))),
    }
}
