//! PulseMQ Native Protocol
//!
//! This is the **primary** wire protocol of PulseMQ.
//! It is designed independently of Kafka and optimized for modern use cases.
//!
//! The Kafka compatibility layer lives in a separate crate (`pulsemq-protocol`).
//!
//! Design goals of the native protocol:
//! - Compact and efficient binary format
//! - First-class support for multi-tenancy (tenant_id in every request)
//! - Built-in tracing / correlation context
//! - Clear versioning and forward compatibility
//! - Lower overhead than legacy protocols where possible

use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

/// Magic bytes that identify a PulseMQ native frame
pub const PULSE_MAGIC: [u8; 4] = [b'P', b'U', b'L', b'S']; // "PULS"

/// Current native protocol version
pub const NATIVE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum NativeApiKey {
    // Core
    Ping = 1,
    Produce = 10,
    Fetch = 11,
    Metadata = 12,

    // Admin
    CreateStream = 20,
    DeleteStream = 21,
    ListStreams = 22,

    // Control
    Heartbeat = 30,
    Join = 31,

    Unknown = 0,
}

impl From<u16> for NativeApiKey {
    fn from(v: u16) -> Self {
        match v {
            1 => NativeApiKey::Ping,
            10 => NativeApiKey::Produce,
            11 => NativeApiKey::Fetch,
            12 => NativeApiKey::Metadata,
            20 => NativeApiKey::CreateStream,
            21 => NativeApiKey::DeleteStream,
            22 => NativeApiKey::ListStreams,
            30 => NativeApiKey::Heartbeat,
            31 => NativeApiKey::Join,
            _ => NativeApiKey::Unknown,
        }
    }
}

/// Native request header — deliberately different from Kafka
#[derive(Debug, Clone)]
pub struct NativeRequestHeader {
    pub version: u16,
    pub api_key: NativeApiKey,
    pub correlation_id: u64,      // wider than Kafka for better uniqueness
    pub tenant_id: u32,           // first-class multi-tenancy
    pub timeout_ms: u32,
}

/// Native response header
#[derive(Debug, Clone)]
pub struct NativeResponseHeader {
    pub correlation_id: u64,
    pub error_code: u16,
}

#[derive(Error, Debug)]
pub enum NativeProtocolError {
    #[error("Invalid magic bytes")]
    InvalidMagic,

    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u16),

    #[error("Incomplete frame")]
    Incomplete,

    #[error("Invalid frame size: {0}")]
    InvalidSize(u32),
}

/// Encode a native frame header
pub fn encode_frame_header(
    buf: &mut BytesMut,
    header: &NativeRequestHeader,
    body_len: u32,
) {
    buf.put_slice(&PULSE_MAGIC);
    buf.put_u16(header.version);
    buf.put_u16(header.api_key as u16);
    buf.put_u64(header.correlation_id);
    buf.put_u32(header.tenant_id);
    buf.put_u32(header.timeout_ms);
    buf.put_u32(body_len);
}

/// Try to decode a native frame header
/// Returns None if not enough data yet
pub fn decode_frame_header(buf: &mut BytesMut) -> Result<Option<(NativeRequestHeader, u32)>, NativeProtocolError> {
    // Minimum size: magic(4) + version(2) + api(2) + corr(8) + tenant(4) + timeout(4) + body_len(4) = 28
    if buf.len() < 28 {
        return Ok(None);
    }

    let magic = [buf.get_u8(), buf.get_u8(), buf.get_u8(), buf.get_u8()];
    if magic != PULSE_MAGIC {
        return Err(NativeProtocolError::InvalidMagic);
    }

    let version = buf.get_u16();
    if version != NATIVE_PROTOCOL_VERSION {
        return Err(NativeProtocolError::UnsupportedVersion(version));
    }

    let api_key = NativeApiKey::from(buf.get_u16());
    let correlation_id = buf.get_u64();
    let tenant_id = buf.get_u32();
    let timeout_ms = buf.get_u32();
    let body_len = buf.get_u32();

    if body_len > 64 * 1024 * 1024 {
        return Err(NativeProtocolError::InvalidSize(body_len));
    }

    Ok(Some((
        NativeRequestHeader {
            version,
            api_key,
            correlation_id,
            tenant_id,
            timeout_ms,
        },
        body_len,
    )))
}

/// Simple ping body (for health checks)
#[derive(Debug, Clone)]
pub struct PingRequest {
    pub payload: Vec<u8>,
}

pub fn protocol_name() -> &'static str {
    "PulseMQ Native Protocol v1"
}
