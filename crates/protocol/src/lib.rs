//! PulseMQ Protocol Layer
//!
//! This crate handles:
//! - Kafka wire protocol compatibility
//! - Message encoding/decoding
//! - Request/Response types
//! - Protocol versioning

use serde::{Deserialize, Serialize};

/// Current protocol version of PulseMQ
pub fn protocol_version() -> &'static str {
    "0.1.0-dev"
}

/// Kafka API Keys (partial list for future compatibility)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    // ... more will be added
}

/// Basic request header (Kafka compatible structure)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

/// Basic response header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

/// Placeholder error codes (Kafka compatible)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    // More error codes will be added
}
