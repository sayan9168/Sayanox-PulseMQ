# Sayanox PulseMQ

**Modern event streaming. Built from first principles.**

PulseMQ is an independent event streaming platform.  
Kafka protocol support exists only as a **compatibility bridge** — not as the core identity.

```text
Compatibility is a feature.
Identity is not negotiable.
```

---

## Why PulseMQ Exists

Most “Kafka alternatives” are still shaped by Kafka’s original decisions.  
PulseMQ starts over with modern requirements:

- Memory-safe high performance (Rust)
- Native multi-tenancy
- Tiered storage as a core idea
- Clean separation between native protocol and compatibility layer
- Simpler operations

---

## Architecture at a Glance

```text
crates/
├── native-protocol/   ← PulseMQ’s own wire protocol (primary)
├── protocol/          ← Kafka compatibility layer only
├── storage/           ← Native storage format (.pmls / .pmidx)
└── broker/            ← Broker process
```

| Layer                | Identity                         |
|----------------------|----------------------------------|
| Native Protocol      | PulseMQ original (`PULS` magic)  |
| Storage Format       | PulseMQ original (`PMLS` magic)  |
| Kafka Protocol       | Optional compatibility only      |

---

## Distinct Storage Format

PulseMQ does **not** use Kafka’s log format.

- Segment files end with `.pmls` (PulseMQ Log Segment)
- Index files end with `.pmidx`
- Every segment starts with magic `PMLS`
- Record layout includes first-class headers and flags
- Directory layout: `data/streams/<name>-<partition>/`

This makes the on-disk format instantly recognizable as PulseMQ.

---

## Native Protocol (New)

`crates/native-protocol` is the primary protocol.

Highlights:
- Magic bytes: `PULS`
- First-class `tenant_id` in every request
- 64-bit correlation IDs
- Clean versioning
- Designed for multi-tenant and cloud-native use

Kafka clients can still connect through the compatibility layer.  
Native clients will use the PulseMQ protocol.

---

## Current Status

| Component              | Status                  |
|------------------------|-------------------------|
| Native Protocol        | Started                 |
| Native Storage Engine  | Working (append)        |
| Kafka Compatibility    | Partial (Metadata, Produce, ApiVersions) |
| Broker                 | Running                 |

---

## Quick Start

```bash
git clone https://github.com/sayan9168/Sayanox-PulseMQ.git
cd Sayanox-PulseMQ
cargo run -p pulsemq-broker
```

---

## Documentation

- [Philosophy](docs/PHILOSOPHY.md) — Why we are not a Kafka clone
- [Differentiation](docs/DIFFERENTIATION.md) — Clear comparison
- [Storage Design](docs/STORAGE_DESIGN.md) — Storage architecture

---

## License

Apache License 2.0

---

**Sayanox PulseMQ**  
Built by [Sayan Mahata](https://github.com/sayan9168) / Sayanox Private Limited

Event streaming, re-imagined.
