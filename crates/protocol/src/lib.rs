//! PulseMQ Protocol Layer — Compatibility Bridge
//!
//! This crate provides **optional** Kafka wire-protocol compatibility.
//!
//! Important:
//! - This is a compatibility layer, not the core identity of PulseMQ.
//! - The internal storage format, APIs, and architecture are designed independently.
//! - Future native PulseMQ protocol can live alongside or replace this layer.
//!
//! Supporting Kafka clients is a migration feature.
//! Being a Kafka clone is not the goal.

use bytes::{Buf, BytesMut};
use thiserror::Error;

/// Current protocol version of PulseMQ
pub fn protocol_version() -> &'static str {
    "0.1.0-dev"
}

/// Kafka API Keys (used only for compatibility)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    Unknown = -1,
}

impl From<i16> for ApiKey {
    fn from(value: i16) -> Self {
        match value {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Metadata,
            18 => ApiKey::ApiVersions,
            19 => ApiKey::CreateTopics,
            20 => ApiKey::DeleteTopics,
            _ => ApiKey::Unknown,
        }
    }
}

/// Request Header (Kafka-compatible structure for the compatibility layer)
#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

/// Response Header
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

/// Error codes (subset used for compatibility responses)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidMessageSize = 4,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    UnsupportedVersion = 35,
    UnknownServerError = -1,
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Not enough data to parse request")]
    Incomplete,

    #[error("Invalid request size: {0}")]
    InvalidSize(i32),

    #[error("Failed to parse client_id")]
    InvalidClientId,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Parse the 4-byte length prefix
pub fn parse_request_size(buf: &mut BytesMut) -> Result<Option<i32>, ProtocolError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let size = buf.get_i32();
    if size < 0 || size > 100 * 1024 * 1024 {
        return Err(ProtocolError::InvalidSize(size));
    }
    Ok(Some(size))
}

/// Parse Request Header from the compatibility layer
pub fn parse_request_header(buf: &mut BytesMut) -> Result<Option<RequestHeader>, ProtocolError> {
    if buf.len() < 10 {
        return Ok(None);
    }

    let api_key = ApiKey::from(buf.get_i16());
    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    if buf.len() < 2 {
        return Ok(None);
    }
    let client_id_len = buf.get_i16();

    if client_id_len < 0 {
        return Err(ProtocolError::InvalidClientId);
    }

    let client_id_len = client_id_len as usize;
    if buf.len() < client_id_len {
        return Ok(None);
    }

    let client_id_bytes = buf.copy_to_bytes(client_id_len);
    let client_id = String::from_utf8_lossy(&client_id_bytes).to_string();

    Ok(Some(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    }))
}
