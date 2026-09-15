# PulseMQ Storage Engine Design

## 1. Overview

PulseMQ uses a **log-structured storage** design inspired by Kafka, with several modern improvements aimed at higher performance and operational simplicity.

### Goals

- Extremely high sequential write throughput
- Low-latency reads (especially sequential)
- Fast random access via sparse indexes
- Support for tiered storage (local + object storage)
- Predictable disk usage via retention & compaction
- Strong durability guarantees

---

## 2. Core Abstractions

### Topic & Partition

- A **Topic** is a logical category of messages.
- Each topic is divided into one or more **Partitions**.
- Each partition is an independent, totally ordered log.

### Log Segment

A partition log is physically split into multiple **segments**:

- Only the **active segment** accepts new writes.
- Older segments are immutable and can be safely moved to cold storage or deleted according to retention policy.

Typical on-disk layout:

```text
data/
└── <topic>-<partition>/
    ├── 00000000000000000000.log
    ├── 00000000000000000000.index
    ├── 00000000000000000000.timeindex
    ├── 00000000000000017384.log
    ├── 00000000000000017384.index
    └── ...
```

- `.log` → actual records
- `.index` → sparse offset → file position mapping
- `.timeindex` → timestamp → offset mapping

---

## 3. Record Format (Planned)

```text
Offset        (8 bytes)   - Logical offset in the partition
MessageSize   (4 bytes)   - Size of the rest of the record
CRC           (4 bytes)   - Checksum
Magic         (1 byte)    - Format version
Attributes    (1 byte)    - Compression, etc.
Timestamp     (8 bytes)
KeyLength     (4 bytes)
Key           (variable)
ValueLength   (4 bytes)
Value         (variable)
Headers       (optional)
```

---

## 4. Indexing Strategy

- **Sparse index**: An index entry is written every N bytes (configurable, default ~4KB).
- Lookup process:
  1. Binary search in the index to find the closest offset ≤ target.
  2. Sequential scan from that position in the `.log` file.

This gives a good balance between index size and lookup speed.

---

## 5. Tiered Storage (Future)

| Tier     | Storage Type       | Latency | Cost   | Use Case                    |
|----------|--------------------|---------|--------|-----------------------------|
| Hot      | Local NVMe / SSD   | Very low| High   | Recent data, active writes  |
| Cold     | Object Storage     | Higher  | Low    | Older segments              |

- Old segments can be automatically uploaded to S3/GCS/MinIO.
- Reads from cold storage will be supported transparently later.

---

## 6. Retention & Compaction

Supported policies (planned):

- **Time-based retention** (`retention.ms`)
- **Size-based retention** (`retention.bytes`)
- **Log Compaction** (keep latest value per key) — later phase

---

## 7. Durability

- Configurable `flush` policy (fsync frequency)
- Write-ahead style: records are appended sequentially
- Crash recovery by scanning the last segment and rebuilding indexes if needed

---

## 8. Implementation Plan (Phased)

### Phase 1 (Current)
- Basic `StorageEngine` + `StorageConfig`
- Directory structure creation
- Skeleton for `Record`

### Phase 2
- Active segment writer (append-only)
- Simple sparse index
- Basic read by offset

### Phase 3
- Multiple segments + rolling
- Time and size based retention
- Recovery logic

### Phase 4
- Tiered storage
- Compaction
- Advanced caching

---

## 9. Performance Targets (Aspirational)

- Write throughput: significantly higher than Kafka on same hardware
- P99 produce latency: very low (sub-millisecond local)
- Efficient sequential and reasonably fast random reads

---

**Document Status**: Living document — will be updated as implementation progresses.
