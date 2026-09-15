//! PulseMQ Storage Engine
//!
//! High-performance log-structured storage with segment-based append.

use bytes::{BufMut, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info};

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

    #[error("Partition not found: {0}-{1}")]
    PartitionNotFound(String, i32),
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

/// A single record to be appended
#[derive(Debug, Clone)]
pub struct Record {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
}

/// Active log segment that supports appending
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
    /// Create a new segment starting at `base_offset`
    pub fn create(dir: &Path, base_offset: i64, index_interval: u32) -> Result<Self, StorageError> {
        std::fs::create_dir_all(dir)?;

        let log_path = dir.join(format!("{:020}.log", base_offset));
        let index_path = dir.join(format!("{:020}.index", base_offset));

        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&log_path)?;

        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&index_path)?;

        info!("Created new segment at base_offset={} path={:?}", base_offset, log_path);

        Ok(Self {
            base_offset,
            log_file,
            index_file,
            current_position: 0,
            bytes_since_index: 0,
            index_interval,
            next_offset: AtomicI64::new(base_offset),
        })
    }

    /// Append a record and return the assigned offset
    pub fn append(&mut self, record: &Record) -> Result<i64, StorageError> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);

        // Simplified record format:
        // [Offset: i64][Timestamp: i64][KeyLen: i32][Key][ValueLen: i32][Value]
        let mut buf = BytesMut::new();
        buf.put_i64(offset);
        buf.put_i64(record.timestamp);

        match &record.key {
            Some(k) => {
                buf.put_i32(k.len() as i32);
                buf.put_slice(k);
            }
            None => {
                buf.put_i32(-1); // null key
            }
        }

        buf.put_i32(record.value.len() as i32);
        buf.put_slice(&record.value);

        let record_bytes = buf.freeze();
        let record_size = record_bytes.len() as u32;

        // Write size + data
        self.log_file.write_all(&record_size.to_be_bytes())?;
        self.log_file.write_all(&record_bytes)?;
        self.log_file.flush()?;

        // Update sparse index if needed
        self.bytes_since_index += record_size + 4;
        if self.bytes_since_index >= self.index_interval {
            // Index entry: relative offset (u32) + position (u32)
            let relative_offset = (offset - self.base_offset) as u32;
            self.index_file.write_all(&relative_offset.to_be_bytes())?;
            self.index_file.write_all(&(self.current_position as u32).to_be_bytes())?;
            self.index_file.flush()?;
            self.bytes_since_index = 0;
        }

        self.current_position += (record_size + 4) as u64;

        debug!("Appended record offset={} size={}", offset, record_size);
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

/// Represents a single partition log (currently one active segment)
pub struct PartitionLog {
    topic: String,
    partition: i32,
    dir: PathBuf,
    active_segment: Mutex<LogSegment>,
    config: StorageConfig,
}

impl PartitionLog {
    pub fn open(topic: &str, partition: i32, config: &StorageConfig) -> Result<Self, StorageError> {
        let dir = config.data_dir.join(format!("{}-{}", topic, partition));
        std::fs::create_dir_all(&dir)?;

        // For now always start a new segment at offset 0.
        // Later we will recover existing segments.
        let segment = LogSegment::create(&dir, 0, config.index_interval_bytes)?;

        Ok(Self {
            topic: topic.to_string(),
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
    // Simple in-memory map for now (will be improved)
    partitions: Mutex<std::collections::HashMap<(String, i32), PartitionLog>>,
}

impl StorageEngine {
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&config.data_dir)?;
        info!("StorageEngine initialized at {:?}", config.data_dir);

        Ok(Self {
            config,
            partitions: Mutex::new(std::collections::HashMap::new()),
        })
    }

    pub fn get_or_create_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(), StorageError> {
        let mut map = self.partitions.lock().unwrap();
        let key = (topic.to_string(), partition);

        if !map.contains_key(&key) {
            let log = PartitionLog::open(topic, partition, &self.config)?;
            map.insert(key, log);
        }
        Ok(())
    }

    pub fn append(
        &self,
        topic: &str,
        partition: i32,
        records: &[Record],
    ) -> Result<Vec<i64>, StorageError> {
        self.get_or_create_partition(topic, partition)?;

        let map = self.partitions.lock().unwrap();
        let key = (topic.to_string(), partition);

        match map.get(&key) {
            Some(log) => log.append(records),
            None => Err(StorageError::PartitionNotFound(topic.to_string(), partition)),
        }
    }

    pub fn log_end_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        let map = self.partitions.lock().unwrap();
        let key = (topic.to_string(), partition);

        match map.get(&key) {
            Some(log) => Ok(log.log_end_offset()),
            None => Ok(0), // empty partition
        }
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }
}

pub fn storage_engine_name() -> &'static str {
    "PulseMQ Storage Engine v0.1"
}
