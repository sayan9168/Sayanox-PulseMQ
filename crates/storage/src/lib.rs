//! PulseMQ Storage Engine
//!
//! High-performance, Kafka-inspired log storage with modern improvements.
//!
//! # Design Goals
//!
//! - Extremely high write throughput
//! - Low latency sequential reads
//! - Efficient random access via sparse indexes
//! - Tiered storage support (hot local + cold object storage)
//! - Strong durability (fsync policies)
//! - Easy compaction and retention
//!
//! # Core Concepts
//!
//! ## Topic / Partition
//! Each topic-partition is an independent ordered log.
//!
//! ## Log Segment
//! A partition log is split into multiple immutable segments.
//! Only the active (latest) segment is open for writing.
//!
//! Typical segment layout on disk:
//!
//! ```text
//! data/
//! └── my-topic-0/
//!     ├── 00000000000000000000.log          # Record data
//!     ├── 00000000000000000000.index        # Offset → position index
//!     ├── 00000000000000000000.timeindex    # Timestamp → offset index
//!     ├── 00000000000000012345.log
//!     ├── 00000000000000012345.index
//!     └── ...
//! ```
//!
//! ## Record Format (simplified)
//!
//! ```text
//! [Offset: i64][MessageSize: i32][CRC: u32][Magic: u8][Attributes: u8]
//! [Timestamp: i64][KeyLength: i32][Key][ValueLength: i32][Value][Headers...]
//! ```
//!
//! ## Index
//! Sparse index mapping logical offset → file position.
//! Allows fast binary search to find the approximate location of any offset.
//!
//! ## Tiered Storage (Future)
//! - Hot tier: local NVMe / SSD (recent segments)
//! - Cold tier: Object storage (S3, GCS, MinIO, etc.)
//! - Automatic migration of old segments to cold storage
//!
//! ## Compaction & Retention
//! - Time-based retention
//! - Size-based retention
//! - Log compaction (keep latest value per key) — future
//!
//! # Planned Public API (skeleton)

use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Segment not found")]
    SegmentNotFound,

    #[error("Offset out of range")]
    OffsetOutOfRange,

    #[error("Invalid record")]
    InvalidRecord,

    #[error("Storage engine not initialized")]
    NotInitialized,
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_max_bytes: u64,      // e.g. 1GB
    pub index_interval_bytes: u32,   // how often to write index entry
    pub retention_ms: Option<u64>,   // time-based retention
    pub retention_bytes: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            segment_max_bytes: 1024 * 1024 * 1024, // 1 GB
            index_interval_bytes: 4096,
            retention_ms: None,
            retention_bytes: None,
        }
    }
}

/// Main entry point of the storage engine
pub struct StorageEngine {
    config: StorageConfig,
    // partitions: HashMap<(String, i32), PartitionLog>,  // future
}

impl StorageEngine {
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&config.data_dir)?;
        Ok(Self { config })
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    // Future methods:
    // pub async fn append(&self, topic: &str, partition: i32, records: &[Record]) -> Result<i64, StorageError>
    // pub async fn read(&self, topic: &str, partition: i32, offset: i64, max_bytes: u32) -> Result<Vec<Record>, StorageError>
    // pub async fn get_log_end_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError>
}

/// Represents a single record in the log (skeleton)
#[derive(Debug, Clone)]
pub struct Record {
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

/// Placeholder name
pub fn storage_engine_name() -> &'static str {
    "PulseMQ Storage Engine v0.1"
}
