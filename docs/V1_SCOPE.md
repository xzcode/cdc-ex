# V1 product scope

Status: M1 implementation in progress; capability matrix describes V1 requirements, not certification.

## Database and event-stream capability matrix

| Connector | Native CDC source | Full snapshot source | Cursor/change-table source | Target |
| --- | --- | --- | --- | --- |
| MySQL | row binlog | yes | not required | keyed upsert/delete |
| MariaDB | row binlog | yes | not required | keyed upsert/delete |
| MongoDB | change streams | yes | not required | document upsert/delete |
| PostgreSQL | pgoutput logical WAL | yes | not required | keyed upsert/delete |
| ClickHouse | no generic row CDC | yes | explicit opt-in | append history or versioned current state |
| Kafka | event-stream consumption (not database CDC) | no | retained topic replay, per-partition offsets | keyed event publication |

These are requirements, not claims of implementation. Each source-to-target pairing requires validated mapping and end-to-end tests. ClickHouse source modes are snapshot, ordered append cursor or explicit producer-maintained changelog. Cursor polling cannot infer arbitrary updates/deletes. Reject unsupported mode combinations before starting.

## User workflows

1. Register a database or Kafka connection; test connectivity, TLS, permissions, topology and CDC prerequisites. Never modify server configuration automatically.
2. Create a one-way task, choose included tables/collections/topics, destination names, key mapping, type mapping, snapshot mode and failure policy. Preview mapping and target DDL.
3. Start, pause gracefully, resume, stop or inspect task; preserve checkpoints. A reset or replacement snapshot requires explicit confirmation.
4. Inspect source lag, sink lag, throughput, queue bytes, journal retention, task state, sanitized errors and blocked events; retry after repair.
5. Validate row/document count and keyed checksums within the mapped scope; report unmatched data. Do not claim consistency for an unsupported transform or an active, uncoordinated comparison.

## V1 boundaries

- One server, one active owner per task. Multiple concurrent tasks and independent sinks; shared source fan-out is optional later.
- One-way only. Block self-targeting and known task cycles. No multi-master conflict resolution or exactly-once promise across heterogeneous databases.
- Keyless source tables: snapshot/append-only where safe; reject state replication unless a stable unique key is configured and validated.
- Table/collection filters, field renames, explicit conversions, null policy, selected exclusions. Filtering on mutable values must handle both enter and leave transitions or be rejected.
- Schema drift: pause on incompatible change. Destructive DDL, truncate and drop never auto-execute. Additive DDL requires explicit policy.
- Credentials encrypted with a protected deployment key, not returned by API or logged. Local-only binding by default, authenticated API, TLS for remote deployment.
- English and Simplified Chinese UI; JAR/Docker distribution.
- Kafka source/sink is optional, never required for direct database synchronization. Topic-to-topic forwarding is supported only with explicit mapping and cycle prevention; no automatic topic creation or offset resets by default. See KAFKA_CONNECTOR.md.
- Other databases and brokers, distributed HA, bidirectional sync and advanced ETL excluded from V1.

## Acceptance

Every advertised capability must have an actual database test demonstrating initial snapshot (if applicable), insert/update/delete, restart and replay, disconnected destination, failed batch, schema drift, key change and expired log handling. Cursor mode has separate boundary/tie and late arrival tests, not native CDC claims. Kafka acceptance additionally requires a real broker test of production, consumption, per-partition ordering, replay, consumer rebalance, offset expiry, broker outage, and invalid payload handling. Report tested database and broker versions rather than declaring all versions compatible.
