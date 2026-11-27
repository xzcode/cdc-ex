# V1 implemented scope and limits

Version `0.1.0-rc.1` is a single-node release candidate. The complete V1 CI gate must pass before publication. See [ACCEPTANCE.md](ACCEPTANCE.md) for the exact fixtures; a provider's availability does not prove every possible source/target pairing.

## Capabilities

| Endpoint | Native capture | Initial data | Destination |
| --- | --- | --- | --- |
| MySQL | ROW/FULL binlog via embedded MySQL connector | initial / no_data | transactional keyed upsert/delete |
| MariaDB | ROW/FULL binlog via independent MariaDB connector | initial / no_data | transactional keyed upsert/delete |
| MongoDB | replica-set Change Streams, required exact pre/post-images | fixed snapshot boundary or no_data | keyed document replace/upsert/delete |
| PostgreSQL | pgoutput, exclusive operator-created slot/publication | initial / no_data | transactional keyed upsert/delete |
| ClickHouse | no generic native CDC | finite snapshot; explicit append cursor; producer changelog | append history / versioned current state |
| Kafka | event stream, partition-local ordering | retained earliest/latest selection, then saved offsets | keyed version-1 CDC EX envelopes |

## User workflows

Register and test source/target connections, create a task with connection references and explicit mapping, validate it, inspect DDL preview, and start it. Managed tasks can pause gracefully, resume, stop or retry after repair while preserving their checkpoints. The UI exposes task state, delivery/journal counters, errors and bounded audit records. DDL preview is read-only; no table, publication, slot or topic is created automatically.

Comparisons require an inactive task, an empty pending queue and the caller's assertion that source writes are quiescent. They compare mapped keyed state within explicit row/byte limits, returning counts and hashed difference samples. They do not establish a distributed snapshot while writers remain active.

## Deliberate boundaries

- One table, collection or topic per task; no fixed active managed task-count limit; actual concurrency depends on resources and connector ownership. A task directory and the management store have one process owner. No distributed lease, HA scheduler or active-active failover.
- One-way tasks. Resource identity checks reject self-targeting, known managed graph cycles and concurrent destination ownership. They cannot infer an unregistered external replication topology.
- At-least-once delivery. Source and sink checkpoints are independent. Source transactions can span delivery batches; heterogeneous sinks are not committed atomically with source offsets.
- Stable explicit keys are required for replicated state. Arbitrary scripts, keyless state replication and filters on mutable values are rejected rather than guessed.
- MySQL/MariaDB initially accept signed integers, exact fixed decimals and supported text columns. PostgreSQL has its own documented exact type allowlist. Unsupported unsigned, binary, spatial, JSON, array or temporal combinations require an explicit implemented mapping, not silent coercion.
- MongoDB requires a replica set with source pre/post-image support, majority durability and sufficient retention. Sharded topology is rejected. Typed BSON wrappers preserve distinctions that plain JSON loses; cross-database conversions are explicit.
- ClickHouse source modes require the documented producer/ordering guarantees. Interrupted finite snapshots do not automatically resume from a different live query. Current-state targets use a dedicated table, durable versions and tombstones; queries require `FINAL` and deletion filtering.
- Kafka consumes only CDC EX version-1 envelopes. No raw forwarding, Debezium JSON decoder, Schema Registry, Avro, Protobuf, compaction tombstones or shared multi-worker group is implemented. Topics must exist; the sink checks broker auto-creation is disabled. Ordering is per partition.
- Incompatible DDL, TRUNCATE, lost/recreated resource identity, expired native offsets, corrupt checkpoints and invalid events block. No automatic skip, implicit latest reset or destructive target DDL.
- Interrupted SQL snapshots or ambiguous first-start failure can require operator reconciliation and a new generation. An unfinished snapshot is never silently restarted against a partly populated target.
- Acknowledged payloads compact into retained replay receipts. Limits count retained logical bytes and lifetime event identities, with additional physical/disk guards. Defaults are 256 MiB and one million identities. There is no indefinite receipt expiry policy; stopped-task limits may be explicitly increased.
- Credentials are encrypted at rest in the control store with a separate AES-256-GCM deployment key. A lost key cannot be reconstructed. Default API binding is loopback; remote binding requires explicit opt-in and a trusted TLS front end.
- The pre-release journal fingerprint is versioned. Earlier development state has no automatic migration into this release candidate. Follow the reconciliation and backup procedure before creating a new task generation.

Dependencies and exact packed hashes are included with each release. The release workload records performance in its stated fixture environment; it is not a general throughput or production availability guarantee.

The post-rc.1 development branch supports optional external management repositories; see [CONTROL_DATABASE.md](CONTROL_DATABASE.md). The published rc.1 compatibility contract is unchanged.
