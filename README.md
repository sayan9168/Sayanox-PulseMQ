# Sayanox PulseMQ

**A modern event streaming platform built from first principles.**

PulseMQ is **not** a Kafka clone.  
It is a new system designed for the next decade of real-time data — with optional Kafka wire-protocol compatibility so existing tools can still talk to it.

---

## Core Philosophy

> Compatibility is a feature. Identity is not negotiable.

- Kafka protocol support = **bridge**, not the foundation
- Internal architecture, storage format, APIs, and operational model are designed independently
- Written in **Rust** for memory safety and predictable performance
- Built for cloud-native, multi-tenant, and edge environments from day one

---

## What Makes PulseMQ Different

| Area                    | PulseMQ Approach                              | Traditional systems          |
|-------------------------|-----------------------------------------------|------------------------------|
| **Core Identity**       | Independent design                            | Often Kafka-derived          |
| **Language**            | Rust (safety + performance)                   | Mostly Java/Scala            |
| **Storage**             | Native tiered storage (hot + cold) planned    | Usually bolted on later      |
| **Multi-tenancy**       | First-class design goal                       | Often afterthought           |
| **Operations**          | Simpler defaults, less ZooKeeper-like complexity | Historically complex     |
| **Edge / Lightweight**  | Planned lightweight mode                     | Heavy by default             |
| **Observability**       | Built-in from the start                       | Usually external             |
| **Protocol**            | Native protocol + optional Kafka compatibility| Kafka protocol is the core   |

---

## Key Design Pillars

1. **Performance with Safety**  
   Rust + careful concurrency design for high throughput without sacrificing reliability.

2. **Operational Simplicity**  
   Fewer moving parts. Better defaults. Clearer failure modes.

3. **Tiered Storage Native**  
   Hot local storage + cold object storage as a core concept, not a later plugin.

4. **Compatibility as a Layer**  
   Kafka clients can connect (via compatibility layer), but the system does not pretend to *be* Kafka internally.

5. **Modern Multi-tenancy**  
   Resource isolation and tenant awareness designed in, not patched later.

---

## Current Status

**Active development** — Core components in progress:

- Broker with TCP listener
- Request parsing (compatibility layer)
- Segment-based storage engine with real append
- Metadata & Produce handling (early stage)
- Clear separation between native design and compatibility layer

---

## Project Structure

```text
crates/
├── broker/     # Main broker process
├── protocol/   # Wire protocol + compatibility layer
└── storage/    # Native log storage engine (independent design)
```

---

## License

Apache License 2.0

---

## About

Built by **Sayanox Private Limited**  
Creator: [Sayan Mahata](https://github.com/sayan9168) — System Architect & Security Researcher

---

**Sayanox PulseMQ** — Event streaming, re-imagined.
