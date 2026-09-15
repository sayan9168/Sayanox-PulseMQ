//! PulseMQ Native Storage Engine
//!
//! This storage format is **intentionally different** from Kafka's log format.
//! It is designed for PulseMQ's own needs (tiered storage readiness,
//! multi-tenancy metadata, modern record layout).

use bytes::{BufMut, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info};

/// Magic number that identifies a PulseMQ segment file
/// "PMLS" = PulseMQ Log Segment
pub const SEGMENT_MAGIC: [u8; 4] = [b'P', b'M', b'L', b'S'];

/// Current on-disk format version
pub const STORAGE_FORMAT_VERSION: u8 = 1;

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

    #[error("Invalid segment magic")]
    InvalidMagic,

    #[error("Unsupported storage format version: {0}")]
    UnsupportedVersion(u8),

    #[error("Storage engine not initialized")]
    NotInitialized,

    #[error("Stream not found: {0}-{1}")]
    StreamNotFound(String, i32),
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub segment_max_bytes: u64,
    pub index_interval_bytes: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            segment_max_bytes: 1024 * 1024 * 1024, // 1 GB
            index_interval_bytes: 4096,
        }
    }
}

/// A single record in PulseMQ native format
#[derive(Debug, Clone)]
pub struct Record {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub headers: Vec<(String, Vec<u8>)>, // first-class headers
}

/// Active log segment (PulseMQ native format)
pub struct LogSegment {
    base_offset: i64,
    log_file: File,
    index_file: File,
    current_position: u64,
    bytes_since_index: u32,
    index_interval: u32,
    next_offset: AtomicI64,
}

impl LogSegment {
    /// Create a new PulseMQ segment
    pub fn create(dir: &Path, base_offset: i64, index_interval: u32) -> Result<Self, StorageError> {
        std::fs::create_dir_all(dir)?;

        // Distinct naming: .pmls (PulseMQ Log Segment) instead of .log
        let log_path = dir.join(format!("{:020}.pmls", base_offset));
        let index_path = dir.join(format!("{:020}.pmidx", base_offset));

        let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&log_path)?;

        // Write segment header (makes format instantly recognizable)
        // [Magic: 4][Version: 1][Reserved: 3][BaseOffset: 8]
        let mut header = BytesMut::with_capacity(16);
        header.put_slice(&SEGMENT_MAGIC);
        header.put_u8(STORAGE_FORMAT_VERSION);
        header.put_bytes(0, 3); // reserved
        header.put_i64(base_offset);
        log_file.write_all(&header)?;
        log_file.flush()?;

        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&index_path)?;

        info!(
            "Created PulseMQ segment base_offset={} path={:?}",
            base_offset, log_path
        );

        Ok(Self {
            base_offset,
            log_file,
            index_file,
            current_position: 16, // after header
            bytes_since_index: 0,
            index_interval,
            next_offset: AtomicI64::new(base_offset),
        })
    }

    /// Append a record in PulseMQ native format and return the assigned offset
    pub fn append(&mut self, record: &Record) -> Result<i64, StorageError> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);

        // PulseMQ Native Record Format (deliberately different from Kafka):
        //
        // [RecordLength: u32]
        // [Offset: i64]
        // [Timestamp: i64]
        // [Flags: u8]          // bit 0 = has_key, bit 1 = has_headers, rest reserved
        // [KeyLength: u32]     // only if has_key
        // [Key: bytes]
        // [ValueLength: u32]
        // [Value: bytes]
        // [HeaderCount: u16]   // only if has_headers
        // [Headers...]         // name_len + name + value_len + value

        let mut body = BytesMut::new();
        body.put_i64(offset);
        body.put_i64(record.timestamp);

        let mut flags: u8 = 0;
        if record.key.is_some() {
            flags |= 0b0000_0001;
        }
        if !record.headers.is_empty() {
            flags |= 0b0000_0010;
        }
        body.put_u8(flags);

        if let Some(ref k) = record.key {
            body.put_u32(k.len() as u32);
            body.put_slice(k);
        }

        body.put_u32(record.value.len() as u32);
        body.put_slice(&record.value);

        if !record.headers.is_empty() {
            body.put_u16(record.headers.len() as u16);
            for (name, value) in &record.headers {
                body.put_u16(name.len() as u16);
                body.put_slice(name.as_bytes());
                body.put_u32(value.len() as u32);
                body.put_slice(value);
            }
        }

        let record_len = body.len() as u32;

        // Write length + body
        self.log_file.write_all(&record_len.to_be_bytes())?;
        self.log_file.write_all(&body)?;
        self.log_file.flush()?;

        // Sparse index entry when interval reached
        self.bytes_since_index += record_len + 4;
        if self.bytes_since_index >= self.index_interval {
            let relative_offset = (offset - self.base_offset) as u32;
            self.index_file.write_all(&relative_offset.to_be_bytes())?;
            self.index_file
                .write_all(&(self.current_position as u32).to_be_bytes())?;
            self.index_file.flush()?;
            self.bytes_since_index = 0;
        }

        self.current_position += (record_len + 4) as u64;

        debug!("Appended PulseMQ record offset={} size={}", offset, record_len);
        Ok(offset)
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.current_position
    }
}

/// A stream partition log (PulseMQ uses "stream" terminology internally)
pub struct StreamLog {
    stream: String,
    partition: i32,
    dir: PathBuf,
    active_segment: Mutex<LogSegment>,
    config: StorageConfig,
}

impl StreamLog {
    pub fn open(stream: &str, partition: i32, config: &StorageConfig) -> Result<Self, StorageError> {
        // Directory naming also distinct: streams/ instead of plain topic-partition
        let dir = config
            .data_dir
            .join("streams")
            .join(format!("{}-{}", stream, partition));
        std::fs::create_dir_all(&dir)?;

        let segment = LogSegment::create(&dir, 0, config.index_interval_bytes)?;

        Ok(Self {
            stream: stream.to_string(),
            partition,
            dir,
            active_segment: Mutex::new(segment),
            config: config.clone(),
        })
    }

    pub fn append(&self, records: &[Record]) -> Result<Vec<i64>, StorageError> {
        let mut segment = self.active_segment.lock().unwrap();
        let mut offsets = Vec::with_capacity(records.len());

        for record in records {
            let offset = segment.append(record)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    pub fn log_end_offset(&self) -> i64 {
        self.active_segment.lock().unwrap().next_offset()
    }
}

/// Main storage engine
pub struct StorageEngine {
    config: StorageConfig,
    streams: Mutex<std::collections::HashMap<(String, i32), StreamLog>>,
}

impl StorageEngine {
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(config.data_dir.join("streams"))?;
        info!("PulseMQ StorageEngine initialized at {:?}", config.data_dir);

        Ok(Self {
            config,
            streams: Mutex::new(std::collections::HashMap::new()),
        })
    }

    pub fn get_or_create_stream(&self, stream: &str, partition: i32) -> Result<(), StorageError> {
        let mut map = self.streams.lock().unwrap();
        let key = (stream.to_string(), partition);

        if !map.contains_key(&key) {
            let log = StreamLog::open(stream, partition, &self.config)?;
            map.insert(key, log);
        }
        Ok(())
    }

    pub fn append(
        &self,
        stream: &str,
        partition: i32,
        records: &[Record],
    ) -> Result<Vec<i64>, StorageError> {
        self.get_or_create_stream(stream, partition)?;

        let map = self.streams.lock().unwrap();
        let key = (stream.to_string(), partition);

        match map.get(&key) {
            Some(log) => log.append(records),
            None => Err(StorageError::StreamNotFound(stream.to_string(), partition)),
        }
    }

    pub fn log_end_offset(&self, stream: &str, partition: i32) -> Result<i64, StorageError> {
        let map = self.streams.lock().unwrap();
        let key = (stream.to_string(), partition);

        match map.get(&key) {
            Some(log) => Ok(log.log_end_offset()),
            None => Ok(0),
        }
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }
}

pub fn storage_engine_name() -> &'static str {
    "PulseMQ Native Storage Engine v0.1"
}
