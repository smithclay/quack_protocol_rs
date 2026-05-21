use crate::binary::{BinaryReader, BinaryWriter, HugeIntParts};
use crate::constants::OPTIONAL_INDEX_INVALID;
use crate::errors::{QuackError, Result};
use crate::logical_types::{LogicalType, decode_logical_type, encode_logical_type};
use crate::vector::{DataChunk, decode_data_chunk_wrapper, encode_data_chunk_wrapper};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub(crate) enum MessageType {
    Invalid = 0,
    ConnectionRequest = 1,
    ConnectionResponse = 2,
    PrepareRequest = 3,
    PrepareResponse = 4,
    FetchRequest = 7,
    FetchResponse = 8,
    AppendRequest = 9,
    SuccessResponse = 10,
    DisconnectMessage = 11,
    ErrorResponse = 100,
}

impl TryFrom<u64> for MessageType {
    type Error = QuackError;

    fn try_from(value: u64) -> Result<Self> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::ConnectionRequest,
            2 => Self::ConnectionResponse,
            3 => Self::PrepareRequest,
            4 => Self::PrepareResponse,
            7 => Self::FetchRequest,
            8 => Self::FetchResponse,
            9 => Self::AppendRequest,
            10 => Self::SuccessResponse,
            11 => Self::DisconnectMessage,
            100 => Self::ErrorResponse,
            _ => {
                return Err(QuackError::protocol(format!(
                    "unknown message type {value}"
                )));
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MessageHeader {
    pub(crate) message_type: MessageType,
    pub(crate) connection_id: Option<String>,
    pub(crate) client_query_id: Option<u64>,
}

impl MessageHeader {
    pub(crate) fn new(message_type: MessageType) -> Self {
        Self {
            message_type,
            connection_id: None,
            client_query_id: None,
        }
    }

    pub(crate) fn with_connection(mut self, connection_id: impl Into<String>) -> Self {
        self.connection_id = Some(connection_id.into());
        self
    }

    pub(crate) fn with_client_query_id(mut self, client_query_id: u64) -> Self {
        self.client_query_id = Some(client_query_id);
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QuackMessage {
    ConnectionRequest {
        header: MessageHeader,
        auth_string: Option<String>,
        client_duckdb_version: Option<String>,
        client_platform: Option<String>,
        min_supported_quack_version: u64,
        max_supported_quack_version: u64,
    },
    ConnectionResponse {
        header: MessageHeader,
        server_duckdb_version: Option<String>,
        server_platform: Option<String>,
        quack_version: Option<u64>,
    },
    PrepareRequest {
        header: MessageHeader,
        sql: String,
    },
    PrepareResponse {
        header: MessageHeader,
        result_types: Vec<LogicalType>,
        result_names: Vec<String>,
        needs_more_fetch: bool,
        results: Vec<DataChunk>,
        result_uuid: HugeIntParts,
    },
    FetchRequest {
        header: MessageHeader,
        result_uuid: HugeIntParts,
    },
    FetchResponse {
        header: MessageHeader,
        results: Vec<DataChunk>,
        batch_index: Option<u64>,
    },
    AppendRequest {
        header: MessageHeader,
        schema_name: Option<String>,
        table_name: String,
        append_chunk: DataChunk,
    },
    SuccessResponse {
        header: MessageHeader,
    },
    Disconnect {
        header: MessageHeader,
    },
    ErrorResponse {
        header: MessageHeader,
        message: String,
    },
}

impl QuackMessage {
    pub(crate) fn header(&self) -> &MessageHeader {
        match self {
            Self::ConnectionRequest { header, .. }
            | Self::ConnectionResponse { header, .. }
            | Self::PrepareRequest { header, .. }
            | Self::PrepareResponse { header, .. }
            | Self::FetchRequest { header, .. }
            | Self::FetchResponse { header, .. }
            | Self::AppendRequest { header, .. }
            | Self::SuccessResponse { header }
            | Self::Disconnect { header }
            | Self::ErrorResponse { header, .. } => header,
        }
    }

    pub(crate) fn message_type(&self) -> MessageType {
        self.header().message_type
    }
}

pub(crate) fn encode_message(message: &QuackMessage) -> Result<Vec<u8>> {
    let mut writer = BinaryWriter::new();
    encode_header(&mut writer, message.header())?;
    encode_body(&mut writer, message)?;
    Ok(writer.into_bytes())
}

pub(crate) fn decode_message(bytes: &[u8]) -> Result<QuackMessage> {
    let mut reader = BinaryReader::new(bytes);
    let header = decode_header(&mut reader)?;
    let message = decode_body(&mut reader, header)?;
    reader.assert_eof()?;
    Ok(message)
}

pub(crate) fn encode_header(writer: &mut BinaryWriter, header: &MessageHeader) -> Result<()> {
    writer.write_object(|object| {
        object.write_field(1, |object| object.write_uleb(header.message_type as u64))?;
        if let Some(connection_id) = header
            .connection_id
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            object.write_field(2, |object| object.write_string(connection_id))?;
        }
        object.write_field(3, |object| {
            object.write_uleb(header.client_query_id.unwrap_or(OPTIONAL_INDEX_INVALID))
        })?;
        Ok(())
    })
}

pub(crate) fn decode_header(reader: &mut BinaryReader<'_>) -> Result<MessageHeader> {
    reader.read_object(|object| {
        let message_type =
            MessageType::try_from(object.read_required_field(1, |object| object.read_uleb_u64())?)?;
        let connection_id = object.read_optional_field(
            2,
            |object| Ok(Some(object.read_string()?)),
            None::<String>,
        )?;
        let client_query_id_raw = object.read_required_field(3, |object| object.read_uleb_u64())?;
        Ok(MessageHeader {
            message_type,
            connection_id: connection_id.filter(|value| !value.is_empty()),
            client_query_id: (client_query_id_raw != OPTIONAL_INDEX_INVALID)
                .then_some(client_query_id_raw),
        })
    })
}

fn encode_body(writer: &mut BinaryWriter, message: &QuackMessage) -> Result<()> {
    match message {
        QuackMessage::ConnectionRequest {
            auth_string,
            client_duckdb_version,
            client_platform,
            min_supported_quack_version,
            max_supported_quack_version,
            ..
        } => writer.write_object(|object| {
            write_optional_string(object, 1, auth_string.as_deref())?;
            write_optional_string(object, 2, client_duckdb_version.as_deref())?;
            write_optional_string(object, 3, client_platform.as_deref())?;
            write_optional_index_default_zero(object, 4, *min_supported_quack_version)?;
            write_optional_index_default_zero(object, 5, *max_supported_quack_version)?;
            Ok(())
        }),
        QuackMessage::ConnectionResponse {
            server_duckdb_version,
            server_platform,
            quack_version,
            ..
        } => writer.write_object(|object| {
            write_optional_string(object, 1, server_duckdb_version.as_deref())?;
            write_optional_string(object, 2, server_platform.as_deref())?;
            if let Some(quack_version) = quack_version {
                object.write_field(3, |object| object.write_uleb(*quack_version))?;
            }
            Ok(())
        }),
        QuackMessage::PrepareRequest { sql, .. } => writer.write_object(|object| {
            write_optional_string(object, 1, Some(sql))?;
            Ok(())
        }),
        QuackMessage::PrepareResponse {
            result_types,
            result_names,
            needs_more_fetch,
            results,
            result_uuid,
            ..
        } => writer.write_object(|object| {
            if !result_types.is_empty() {
                object.write_field(1, |object| {
                    object.write_list(result_types, |object, logical_type, _| {
                        encode_logical_type(object, logical_type)
                    })
                })?;
            }
            if !result_names.is_empty() {
                object.write_field(2, |object| {
                    object.write_list(result_names, |object, name, _| object.write_string(name))
                })?;
            }
            if *needs_more_fetch {
                object.write_field(3, |object| object.write_bool(true))?;
            }
            if !results.is_empty() {
                object.write_field(4, |object| write_chunk_pointer_list(object, results))?;
            }
            object.write_field(5, |object| object.write_huge_int_parts(*result_uuid))?;
            Ok(())
        }),
        QuackMessage::FetchRequest { result_uuid, .. } => writer.write_object(|object| {
            object.write_field(1, |object| object.write_huge_int_parts(*result_uuid))?;
            Ok(())
        }),
        QuackMessage::FetchResponse {
            results,
            batch_index,
            ..
        } => writer.write_object(|object| {
            if !results.is_empty() {
                object.write_field(1, |object| write_chunk_pointer_list(object, results))?;
            }
            object.write_field(2, |object| {
                object.write_uleb(batch_index.unwrap_or(OPTIONAL_INDEX_INVALID))
            })?;
            Ok(())
        }),
        QuackMessage::AppendRequest {
            schema_name,
            table_name,
            append_chunk,
            ..
        } => writer.write_object(|object| {
            write_optional_string(object, 1, schema_name.as_deref())?;
            write_optional_string(object, 2, Some(table_name))?;
            object.write_field(3, |object| {
                object.write_nullable(Some(append_chunk), encode_data_chunk_wrapper)
            })?;
            Ok(())
        }),
        QuackMessage::SuccessResponse { .. } | QuackMessage::Disconnect { .. } => {
            writer.write_object(|_| Ok(()))
        }
        QuackMessage::ErrorResponse { message, .. } => writer.write_object(|object| {
            write_optional_string(object, 1, Some(message))?;
            Ok(())
        }),
    }
}

fn decode_body(reader: &mut BinaryReader<'_>, header: MessageHeader) -> Result<QuackMessage> {
    match header.message_type {
        MessageType::ConnectionRequest => reader.read_object(|object| {
            Ok(QuackMessage::ConnectionRequest {
                header,
                auth_string: read_optional_string(object, 1)?,
                client_duckdb_version: read_optional_string(object, 2)?,
                client_platform: read_optional_string(object, 3)?,
                min_supported_quack_version: object.read_optional_field(
                    4,
                    |object| object.read_uleb_u64(),
                    0,
                )?,
                max_supported_quack_version: object.read_optional_field(
                    5,
                    |object| object.read_uleb_u64(),
                    0,
                )?,
            })
        }),
        MessageType::ConnectionResponse => reader.read_object(|object| {
            Ok(QuackMessage::ConnectionResponse {
                header,
                server_duckdb_version: read_optional_string(object, 1)?,
                server_platform: read_optional_string(object, 2)?,
                quack_version: object.read_optional_field(
                    3,
                    |object| Ok(Some(object.read_uleb_u64()?)),
                    None::<u64>,
                )?,
            })
        }),
        MessageType::PrepareRequest => reader.read_object(|object| {
            Ok(QuackMessage::PrepareRequest {
                header,
                sql: object.read_optional_field(1, |object| object.read_string(), String::new())?,
            })
        }),
        MessageType::PrepareResponse => reader.read_object(|object| {
            Ok(QuackMessage::PrepareResponse {
                header,
                result_types: object.read_optional_field(
                    1,
                    |object| object.read_list(|object, _| decode_logical_type(object)),
                    Vec::new(),
                )?,
                result_names: object.read_optional_field(
                    2,
                    |object| object.read_list(|object, _| object.read_string()),
                    Vec::new(),
                )?,
                needs_more_fetch: object.read_optional_field(
                    3,
                    |object| object.read_bool(),
                    false,
                )?,
                results: object.read_optional_field(4, read_chunk_pointer_list, Vec::new())?,
                result_uuid: object
                    .read_required_field(5, |object| object.read_huge_int_parts())?,
            })
        }),
        MessageType::FetchRequest => reader.read_object(|object| {
            Ok(QuackMessage::FetchRequest {
                header,
                result_uuid: object
                    .read_required_field(1, |object| object.read_huge_int_parts())?,
            })
        }),
        MessageType::FetchResponse => reader.read_object(|object| {
            let results = object.read_optional_field(1, read_chunk_pointer_list, Vec::new())?;
            let batch_index = object.read_required_field(2, |object| object.read_uleb_u64())?;
            Ok(QuackMessage::FetchResponse {
                header,
                results,
                batch_index: (batch_index != OPTIONAL_INDEX_INVALID).then_some(batch_index),
            })
        }),
        MessageType::AppendRequest => reader.read_object(|object| {
            let schema_name = read_optional_string(object, 1)?;
            let table_name =
                object.read_optional_field(2, |object| object.read_string(), String::new())?;
            let append_chunk = object.read_optional_field(
                3,
                |object| object.read_nullable(decode_data_chunk_wrapper),
                None,
            )?;
            Ok(QuackMessage::AppendRequest {
                header,
                schema_name,
                table_name,
                append_chunk: append_chunk.ok_or_else(|| {
                    QuackError::protocol("APPEND_REQUEST is missing append_chunk")
                })?,
            })
        }),
        MessageType::SuccessResponse => {
            reader.read_object(|_| Ok(QuackMessage::SuccessResponse { header }))
        }
        MessageType::DisconnectMessage => {
            reader.read_object(|_| Ok(QuackMessage::Disconnect { header }))
        }
        MessageType::ErrorResponse => reader.read_object(|object| {
            Ok(QuackMessage::ErrorResponse {
                header,
                message: object.read_optional_field(
                    1,
                    |object| object.read_string(),
                    String::new(),
                )?,
            })
        }),
        other => Err(QuackError::protocol(format!(
            "cannot decode unsupported message type {other:?}"
        ))),
    }
}

fn read_chunk_pointer_list(reader: &mut BinaryReader<'_>) -> Result<Vec<DataChunk>> {
    reader.read_list(|reader, _| {
        reader
            .read_nullable(decode_data_chunk_wrapper)?
            .ok_or_else(|| {
                QuackError::protocol("encountered null DataChunk pointer in result list")
            })
    })
}

fn write_chunk_pointer_list(writer: &mut BinaryWriter, chunks: &[DataChunk]) -> Result<()> {
    writer.write_list(chunks, |writer, chunk, _| {
        writer.write_nullable(Some(chunk), encode_data_chunk_wrapper)
    })
}

fn write_optional_string(
    writer: &mut BinaryWriter,
    field_id: u16,
    value: Option<&str>,
) -> Result<()> {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        writer.write_field(field_id, |writer| writer.write_string(value))?;
    }
    Ok(())
}

fn write_optional_index_default_zero(
    writer: &mut BinaryWriter,
    field_id: u16,
    value: u64,
) -> Result<()> {
    if value != 0 {
        writer.write_field(field_id, |writer| writer.write_uleb(value))?;
    }
    Ok(())
}

fn read_optional_string(reader: &mut BinaryReader<'_>, field_id: u16) -> Result<Option<String>> {
    reader.read_optional_field(
        field_id,
        |reader| Ok(Some(reader.read_string()?)),
        None::<String>,
    )
}
