# Architecture and durability

## Runtime

Java 21 + Maven + Spring Boot, embedded Debezium capture for four native sources, JDBC/native drivers for targets. Vue 3 + TypeScript management UI served by the application. SQLite metadata + bounded durable journal in a persistent data directory. Pin and verify dependency versions before implementation; no mandatory Kafka or Flink.

```text
Source connector -> normalize -> durable journal -> mapping -> sink writer
                        |             |                       |
                  source offset   replay + limits       sink checkpoint
Control API -> task supervisor -> metrics / errors / connector preflight
```

Modules: cdc-common (typed events), cdc-connector-api (SPI), cdc-storage (journal and checkpoints), cdc-runtime (supervisor and delivery), cdc-transform (mappings), cdc-connectors/{mysql,mariadb,mongodb,postgresql,clickhouse}, cdc-server (API), cdc-web (UI), cdc-distribution (JAR/container). Isolate connectors and do not expose Debezium-specific types in public task API.

## Event model

Each event includes task generation, source identity, namespace, operation (SNAPSHOT, INSERT, UPDATE, DELETE, SCHEMA_CHANGE, TRUNCATE), opaque partition and offset, stable event ID, key/old key, before/after or patch, schema version, commit timestamp if available, ingestion timestamp, transaction metadata if available. Missing fields and explicit null are different. Preserve precision and typed BSON/binary/decimal/timestamp values. Do not invent absent before-images. Source offsets are opaque and are never compared across databases.

## Reliability invariants

- Persist the entire accepted capture batch in a durable transaction before acknowledging records to the source engine. Store connector-specific offset and schema history safely; never advance the source beyond durable events. A crash between journal commit and offset flush can replay; deduplicate by stable source event identity within a task generation. Snapshot replay needs explicit epoch and per-key identity rules.
- Each sink has a separate contiguous acknowledgement watermark. Advance it only after confirmed target commit; an ambiguous commit retries using deterministic event identity and idempotent target writes where supported.
- Start with serial delivery per sink, preserving source/key order; preserve transaction boundaries where available and compatible. Explicitly disclose any cross-table or cross-shard ordering limitation.
- Use target transactions where possible, composite primary keys, parameter binding and safe identifier quoting. Primary key change means ordered delete(old key) plus upsert(new key). Never construct predicates from untrusted SQL text.
- Bound queue and batch size; use durable writes (SQLite WAL and appropriate synchronous setting). Apply backpressure before disk exhaustion; report source log retention risk. Do not drop an event or advance past an unresolved failure by default.
- Persist schema history and task config generation; block incompatible config changes and recovery after lost offsets or purged source logs. Never silently jump to current offset.
- Snapshot must have documented source-consistent boundary and replay ordering. Do not implement as naive full query followed by starting at latest offset. Interrupted snapshot must resume safely or restart an explicit generation; reconciliation must handle target rows absent from the new snapshot.
- Journal cleanup after all relevant durable sink checkpoints and safe replay horizon. Redact secrets and payloads from normal logs. Data directory is sensitive and needs restricted permissions, disk encryption/backup policy.

## Task lifecycle

DRAFT -> VALIDATING -> STARTING -> SNAPSHOTTING or STREAMING -> PAUSING -> PAUSED; STOPPING -> STOPPED; recoverable errors -> RETRYING; nonrecoverable errors -> BLOCKED or FAILED. Capture and delivery progress visible independently. Configuration and runtime state are separate. Restart recovery never implicitly resets offsets.

## Deployment

One JVM/container with a mounted persistent volume and a protected encryption key, optionally behind a reverse proxy. No default public bind with unauthenticated access. Database connectivity and source configuration are external prerequisites. HA, external journal and distributed ownership are later designs.

## References

- https://debezium.io/documentation/reference/stable/development/engine.html
- https://debezium.io/documentation/reference/stable/connectors/
- https://clickhouse.com/blog/10-best-practice-tips
