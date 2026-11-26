# CDC EX

[简体中文](docs/README.zh-CN.md)

A lightweight, self-hosted change data capture and database synchronization platform. CDC EX owns its task orchestration, durable delivery, mappings and management UI, while reusing mature database capture libraries.

**Status:** M9 integration candidate. Native MySQL, MariaDB, MongoDB and PostgreSQL capture, limited ClickHouse source modes, optional Kafka endpoints, authenticated task management, encrypted connection storage and an English/Chinese web UI are implemented. Final capability acceptance is in progress; see [M9 handoff](docs/HANDOFF_M9.md). No blanket production certification.

## V1 database and event-stream scope

| Database | Source | Sink | Capture |
| --- | --- | --- | --- |
| MySQL | Implemented | Implemented | Row-based binlog |
| MariaDB | Implemented | Implemented | Row-based binlog, MariaDB-specific offsets |
| MongoDB | Implemented | Implemented | Change Streams (replica set or sharded cluster) |
| PostgreSQL | Implemented | Implemented | Logical WAL decoding (pgoutput) |
| ClickHouse | Limited modes | Implemented | Snapshot, explicitly configured incremental cursor or application-owned change table |
| Apache Kafka | Implemented | Implemented | Topic / partition / offset, explicit message decoding |

ClickHouse is primarily an analytical sink. Its ordinary tables do not expose a general row-level CDC stream equivalent to MySQL binlog. Cursor mode is not full CDC and does not discover arbitrary deletes or mutations. An explicit change table can represent deletes and updates only if the producer reliably records them.

Kafka is an optional source and sink, not a mandatory infrastructure dependency. Implemented modes include database-to-Kafka CDC publication, Kafka-to-database ingestion and Kafka-to-Kafka forwarding. See [Kafka connector specification](docs/KAFKA_CONNECTOR.md).

## Design

- Java 21, Maven, Spring Boot, embedded Debezium, Vue 3 / TypeScript.
- Single-node deployment first: one process or container, no mandatory Kafka/Flink.
- SQLite metadata and bounded durable event journal; independent source and sink checkpoints.
- At-least-once delivery, deterministic replay, idempotent target writes where supported.
- One-way tasks, explicit key and type mapping, no destructive DDL by default.
- Full snapshot, native CDC, and cursor polling are distinct capability flags.
- English by default; Simplified Chinese supported.

## Roadmap

See [V1 scope](docs/V1_SCOPE.md), [architecture](docs/ARCHITECTURE.md), [connector contract](docs/CONNECTORS.md) and [development roadmap](docs/ROADMAP.md).

## License

License selection pending; Apache-2.0 is proposed. Third-party components retain their own licenses.
