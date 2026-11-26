# Architecture and durability

CDC EX is a single-node Java 21 application with a bounded SQLite event journal, optional authenticated management API and a Vue 3 / TypeScript UI. MySQL, MariaDB and PostgreSQL sources embed Debezium; MongoDB uses the native synchronous driver and Change Streams. ClickHouse uses a restricted HTTP client for limited source modes and envelope sinks. Kafka is an optional Java-client endpoint, not a required transport for direct database tasks.

Operational behavior is described in [RUNBOOK.md](RUNBOOK.md), transformations in [MAPPINGS.md](MAPPINGS.md), and observed compatibility in [ACCEPTANCE.md](ACCEPTANCE.md). This document describes the implemented architecture rather than a future capability wishlist.

## Modules

| Path | Responsibility |
| --- | --- |
| `cdc-core` | Typed events, endpoint/provider SPI, ordered sink sequences, JSON codec and sanitized failures |
| `cdc-storage` | SQLite durable journal, permanent replay receipts, encrypted control metadata |
| `cdc-debezium` | Checksummed atomic offset store and shared capture metadata helpers |
| `cdc-runtime` | Task configuration/fingerprint, deterministic mappings, capture/delivery coordination |
| `cdc-mysql`, `cdc-mariadb` | Separate binlog connectors and transactional SQL targets |
| `cdc-mongodb` | Fixed-timestamp snapshot, exact Change Stream images, transactional BSON replacement |
| `cdc-postgresql` | pgoutput capture, slot/schema guards, typed transactional target |
| `cdc-clickhouse` | Finite snapshot, append cursor, producer changelog, versioned/history envelopes |
| `cdc-kafka` | CDC_EX/1 topic capture/publication, partition checkpoints and TLS/SASL |
| `cdc-server` | CLI, Spring Boot API, ownership, task manager, comparison, DDL preview |
| `cdc-web` | English/Chinese management UI |
| `cdc-distribution`, root Docker/Compose files | Container health check and deployment packaging |

Connectors implement `ConnectorProvider` for capabilities, inspection, source/sink factories and optional bounded row reads. Debezium-specific records do not appear in the public task API. A task selects one source resource and one target resource; the manager rejects known cycles and concurrent active ownership of the same target.

## Durable event flow

```mermaid
sequenceDiagram
    participant S as Source connector
    participant J as Durable journal
    participant W as Sink writer
    participant T as Destination
    S->>J: Append complete capture batch
    J-->>S: Confirm durable acceptance
    Note over S: Advance source checkpoint
    J->>W: Read pending durable sequences
    W->>T: Apply mapped batch
    T-->>W: Confirm target commit
    W->>J: Acknowledge contiguous sequences
    Note over J: Compact payloads; retain identity receipts
```

The journal uses SQLite WAL with `synchronous=FULL`. Source acknowledgement occurs only after the accepted batch commits locally. Source checkpoint writes can lag that commit, so replay must be safe. Event IDs and exact ordered payload digests detect an identity reused with different content. Delivered payloads are compacted into permanent receipts; IDs are not silently expired. The journal enforces logical/lifetime limits and physical/free-space guards, described in the operator runbook.

Mapping runs at delivery over the stored original event. The task fingerprint pins resource identity, relevant schema, endpoint structure, source mode and mapping. Resource limits can be updated while inactive without replacing positions. Password-field rotation does not replace the task generation; opaque credential-bearing URI changes may affect its fingerprint.

The sink acknowledges only after confirmed application. A process failure between destination commit and journal acknowledgement may repeat the batch. SQL/MongoDB use transactional primary-key writes; ClickHouse current-state versions derive from the durable journal sequence; Kafka and history publication can retain replay duplicates. Delivery is at least once. Source transactions can span batches, and cross-table/cross-system/cross-partition atomicity is not promised.

## Event and type model

`ChangeEvent` contains `id`, `database`, `table`, `operation`, opaque `position`, `key`, `before`, `after` and `sourceTimeMillis`. Normalized state operations are SNAPSHOT, INSERT, UPDATE and DELETE. TRUNCATE and unsupported events block instead of being silently treated as ordinary writes. There is no claim that the event carries a full distributed transaction, an ingestion timestamp or a universal schema-evolution protocol.

Missing values and explicit null remain distinct. Integers/decimals use exact Java representations; native MongoDB scalars use canonical Extended JSON and preserve nested document order. Source positions remain connector-specific. Mappings explicitly rename, exclude or convert top-level fields; no scripts, row predicates or implicit flattening are implemented. Destination value/schema checks reject unsupported types and lossy writes.

## Snapshot and recovery boundaries

MySQL/MariaDB and PostgreSQL use their documented consistent Debezium snapshot/log handoff. An interrupted initial scan blocks automatic resume; a completed snapshot can restart without waiting for a live DML event. MongoDB persists a fixed snapshot `atClusterTime` before emitting rows and resumes that same timestamp while retained history permits, followed by exact required pre/post-images. Expired history blocks.

ClickHouse finite snapshot is one streaming SELECT. It has no durable server-side point that can reconstruct an interrupted query, so that case requires a reconciled generation. Cursor mode requires immutable monotonic-visible tuples; changelog mode requires contiguous commit-visible sequence numbers. Neither infers arbitrary ClickHouse mutations. Kafka persists first-start offsets and then resumes authoritative local partition offsets, with explicit topic identity and retention checks.

Missing/corrupt state, resource replacement, incompatible schema or retained-history loss blocks rather than silently choosing the current position. Selected-table DDL is not applied to a target. Any new generation needs an explicit destination reconciliation plan, including stale rows that a fresh source scan would not remove.

## Management, security and lifecycle

The server stores encrypted connection/task JSON in `control.db`; task journals and connector files live under `tasks/<id>/`. AES-256-GCM configuration encryption does not encrypt pending event data. A deployment key is verified before reading existing control state. A bearer token protects all control APIs; public health and static UI assets expose no connection secrets. The default bind is loopback. See [SECURITY.md](SECURITY.md).

A server directory and each task directory have exclusive process locks. The managed service supports at most four active tasks. Startup/retry reaches CAPTURING or BLOCKED; temporary sink errors use RETRYING. Pause/stop are asynchronous and preserve pending work. After service restart, interrupted active tasks are marked STOPPED for explicit review/resume, not automatically restarted. The runtime does not expose a distinct, universally reliable snapshot-versus-streaming state.

Comparison requires an inactive task, no pending journal events and an operator assertion that source writes are quiescent. It performs independent bounded reads, applies the task mapping, and reports keyed counts/hashes. It is not a distributed snapshot or an online repair mechanism. DDL preview is explicitly non-executable. HA, distributed ownership, online backup, automatic key migration and automatic schema evolution remain outside this implementation.

## 简体中文

当前架构为单节点 JVM、SQLite 持久日志和可选管理界面。MySQL、MariaDB、PostgreSQL 使用 Debezium，MongoDB 使用原生 Change Streams；ClickHouse 仅提供明确受限的源模式，Kafka 不是直连同步的必选依赖。

完整批次先进入持久日志，再推进源位点；目标提交后才推进投递水位。已投递载荷会压缩为永久去重凭据。任务映射、资源身份与结构被固定，故障后按原状态重放；目标已提交但本地未确认的窗口仍可能重复，所以不是跨系统恰好一次。位点损坏、历史过期或资源变化不会触发静默重置。
