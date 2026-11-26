# CDC EX

[简体中文](docs/README.zh-CN.md)

A lightweight, self-hosted change data capture and database synchronization platform. CDC EX owns its task orchestration, durable delivery, mappings and management UI, while reusing mature database capture libraries.

**Status:** M0 design baseline; no connector is implemented or production-certified yet.

## V1 database scope

| Database | Source | Sink | Capture |
| --- | --- | --- | --- |
| MySQL | Planned | Planned | Row-based binlog |
| MariaDB | Planned | Planned | Row-based binlog, MariaDB-specific offsets |
| MongoDB | Planned | Planned | Change Streams (replica set or sharded cluster) |
| PostgreSQL | Planned | Planned | Logical WAL decoding (pgoutput) |
| ClickHouse | Limited, planned | Planned | Snapshot, explicitly configured incremental cursor or application-owned change table |

ClickHouse is primarily an analytical sink. Its ordinary tables do not expose a general row-level CDC stream equivalent to MySQL binlog. Cursor mode is not full CDC and does not discover arbitrary deletes or mutations. An explicit change table can represent deletes and updates only if the producer reliably records them.

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
