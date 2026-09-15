# How PulseMQ is Different from Kafka

This document exists so no one can reasonably claim PulseMQ is “just a Kafka copy”.

---

## 1. Architectural Independence

| Component            | Kafka                          | PulseMQ                              |
|----------------------|--------------------------------|--------------------------------------|
| Core Language        | Java / Scala                   | **Rust**                             |
| Coordination         | Historically ZooKeeper (now KRaft) | Independent design (Raft planned) |
| Storage Format       | Kafka log format               | **Native segment + record format**   |
| Protocol             | Kafka protocol *is* the system | Kafka protocol is a **compatibility layer** |
| Multi-tenancy        | Limited / external             | Designed as a core goal              |
| Tiered Storage       | Added later / external         | Planned as native capability         |

---

## 2. Explicit Compatibility Layer

In PulseMQ the Kafka wire protocol lives in `crates/protocol`.

It is deliberately separated from:

- The storage engine (`crates/storage`)
- The broker core logic
- Future native client protocol

This separation is intentional. The system can evolve its internal protocol and storage without being permanently tied to Kafka’s design decisions.

---

## 3. Native Storage Design

PulseMQ storage is **not** a reimplementation of Kafka’s log format.

Current design choices:

- Independent record layout
- Sparse indexing strategy under our control
- Segment management designed for future tiered storage
- Clear path to hot/cold separation

---

## 4. Different Priorities

Kafka optimized for:
- Extremely high throughput in the datacenter
- Strong ordering guarantees
- Broad ecosystem (which became its strength and also its constraint)

PulseMQ prioritizes:
- Memory safety + performance (Rust)
- Operational simplicity
- Native multi-tenancy
- Tiered storage from early stages
- Clean separation between compatibility and core
- Modern cloud + edge readiness

---

## 5. Positioning Statement

> PulseMQ can accept traffic from Kafka clients.  
> That does not make it a Kafka distribution or a Kafka clone.

It is a new event streaming platform that chooses to offer a migration bridge.

---

## 6. What We Will Never Do

- Pretend to be “Kafka but faster” as the only identity
- Copy Kafka’s internal module structure or class hierarchy
- Make the Kafka protocol the permanent center of the architecture
- Inherit operational complexity without questioning it

---

**Bottom line:**  
Compatibility is a feature.  
Being a clone is not.
