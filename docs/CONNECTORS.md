# Implemented connector contract

The Java interfaces are in `cdc-core`; providers are registered by the server's `ConnectorRegistry`. They expose implemented modes, not a promise of universal database compatibility. Each provider inspects its concrete physical resource and prerequisites before a task starts.

## SPI

`ConnectorProvider` provides `type()`, `capabilities()`, `inspect(Endpoint, Role)`, `openSource(Endpoint, SourceContext)` and `openSink(Endpoint, SinkContext)`. Its optional `readRows(Endpoint, limit)` supplies a bounded validation scan. An `Inspection` contains a non-secret physical identity, ordered field types, key fields and non-secret schema/topology details. Task fingerprints include identities, schema and mapping; credentials are not exposed through diagnostics.

`SourceConnector.run(DurableReceiver)` emits an ordered batch and may acknowledge the native position only after `receiver.accept(batch)` returns successfully. An internal heartbeat can persist an empty snapshot's completed offset without fabricating a business row. Unknown metadata or operations are handled explicitly. `finite()` identifies a reader that can finish, after which the runtime drains its durable queue.

`SinkConnector.apply(batch)` returns only after all supported writes have been confirmed. The runtime retries an ambiguous or failed delivery without advancing its sink watermark. `SequencedSinkConnector` additionally receives the stable SQLite journal sequence; ClickHouse uses it for deterministic versions across replay. Source transactions are not preserved as an atomic unit across arbitrary sink batches.

`ChangeEvent` contains stable ID, database/collection namespace, table, operation, opaque source position, key, before, after and source timestamp. Nested values are immutable JSON-compatible values; exact integers/decimals and explicit BSON/type tags retain distinctions. `EventCodec.canonicalJson` is reserved for canonical identities; normal payload JSON preserves document field order.

## Source contracts

| Source | Required setup | Durable resume identity |
| --- | --- | --- |
| MySQL | ROW/FULL binlog, InnoDB, allowed types, read/replication/metadata privileges, unique replication client ID | checksummed Debezium offset + schema history + physical table identity |
| MariaDB | dedicated MariaDB connector; ROW/FULL; legacy event position enabled and log compression disabled | MariaDB connector offset + schema history + physical identity |
| MongoDB | replica set, exact pre/post-images, readable history and snapshot boundary | replica-set ID + collection UUID + native resume token |
| PostgreSQL | logical WAL, UTF-8, FULL replica identity, operator-created exclusive pgoutput slot/publication | system identifier + database/table identity + slot boundary + native LSN state |
| ClickHouse | Atomic database/table UUID; allowed local table/types; mode-specific guarantees | table UUID/schema + snapshot status or exact cursor/changelog position |
| Kafka | explicit topic, task-exclusive group, CDC EX envelopes, required metadata/read/group permissions | cluster/topic IDs + next offset per partition |

The provider revalidates relevant schema and resource identity at delivery or capture boundaries. This detects supported local resource changes; it is not a general cross-cluster failover protocol. Lost retention does not cause a silent resnapshot or latest-offset reset.

## Target contracts

SQL targets use parameterized JDBC transactions and explicit column lists. They validate types, precision, key shape and schema; unsupported narrowing, rounding and coercion block. MySQL/MariaDB text keys require supported binary equality semantics. PostgreSQL uses its documented type encodings and constraint checks.

MongoDB replaces the mapped document by `_id`, with majority writes and BSON reconstruction. Replacement semantics intentionally remove unmapped fields; mappings must describe the desired target document. It does not apply `updateLookup` results as if they were historical exact post-images.

ClickHouse targets accept the documented envelope schemas. History retains event IDs and may contain replay duplicates. Current-state mode requires exclusive ownership, an empty new target generation and `ReplacingMergeTree(version,is_deleted)` with `ORDER BY entity_key`, no partition expression and required fsync settings. Logical reads use `FINAL` and exclude tombstones.

Kafka publishes keyed CDC EX envelopes with `acks=all`, idempotent producer configuration and one in-flight request per connection. Process-level replay can still duplicate messages. Topic identity and destination partition count are pinned. TLS and supported SASL mechanisms are configured through a fixed option allowlist, not arbitrary JAAS text.

## Adding a connector

Implement the SPI with explicit prerequisites, stable physical identity, exact data encodings and a durable resume contract. Register it, add typed connection fields/capabilities to the UI, provide mapping fixtures and an isolated real-service acceptance script. Test capture→journal, journal→native checkpoint and target→sink watermark crash boundaries relevant to that connector. Add the exact fixture to V1 CI before advertising it.

Detailed contracts: [MySQL](M1_RUNBOOK.md), [MariaDB](M2_MARIADB.md), [MongoDB](M3_MONGODB.md), [PostgreSQL](M4_POSTGRESQL.md), [ClickHouse](M5_CLICKHOUSE.md), [Kafka](M5K_KAFKA.md), [mappings](MAPPINGS.md).
