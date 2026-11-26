# M9 integration handoff

Work is proceeding through M9 without a milestone-by-milestone approval pause. This file is a checkpoint, not a completed acceptance certificate.

## Implemented

- Shared provider SPI, explicit endpoint/task configuration, one-way multi-task supervisor, independent durable capture and sink watermarks.
- Atomic, checksummed, fsynced Debezium offset store; acknowledged payload compaction with retained replay receipts; logical capacity, physical journal and disk-reserve guards.
- MySQL and independent MariaDB binlog capture/sinks; MongoDB fixed-boundary snapshot and exact-image Change Streams; PostgreSQL pgoutput; Kafka source/sink; ClickHouse envelope sinks and limited snapshot/cursor/changelog readers.
- Explicit field rename/exclusion/conversion, exact decimal/integer checks, bounded quiescent comparison, DDL preview only, source/target identity and schema fencing.
- Authenticated HTTP control plane, AES-256-GCM connection storage, separate deployment key, process/task locks, cycle and destination-ownership checks, safe diagnostics.
- Vue/TypeScript UI, English and Simplified Chinese, connection preflight, task creation/control, metrics and audit views.
- JAR, pinned-base Dockerfile, Compose configuration and disposable acceptance scripts.

## Verified at this checkpoint (2026-11-26)

- Prior commit 678da789: GitHub Actions run 45785635121 passed all four MySQL/MariaDB source/sink combinations. Actual versions: MySQL 8.4.13 and MariaDB 11.4.13.
- Full default Java 21 Maven builds on the integration host succeeded through all 12 modules, including the new storage, mapping and connector tests available at those snapshots.
- PostgreSQL 17.7 real snapshot, exact types, TOAST, DML, key changes, clean/hard-crash restart, retained retries, schema drift and missing-slot rejection passed.
- MongoDB 8.0.15 real snapshot, BSON fidelity, exact update images, delete, graceful/hard-crash recovery, destination outage and expired boundary rejection passed.
- Kafka 4.0.0 with client 3.9.1 passed real partition/rebalance, checkpoint replay/expiry, topic recreation and invalid payload cases. TLS and SASL PLAIN/SCRAM transport cases also passed.
- Real packaged-server security checks passed locally: authentication, encrypted credential storage/redaction, ownership, deployment-key rejection and restricted binding.
- Frontend production build passed. Real UI/database acceptance and the final aggregate CI commit remain gates.

## Remaining acceptance work

Complete the latest-code SQL matrix, ClickHouse native fixtures, SQL/Kafka full chain, mapped heterogeneous paths, real UI task flow, bounded performance/fault workload, Docker smoke, final CI and release artifacts. Update this checkpoint with exact final commit, workflow, test counts and measured results after they pass.

## Deliberate limits

One table/collection/topic per task; four concurrent managed tasks per node. No distributed HA, multi-master replication, automatic destructive DDL, raw Kafka forwarding, arbitrary scripts or end-to-end exactly-once promise. MySQL/MariaDB SQL types are a strict signed-integer/fixed-decimal/text subset. MongoDB requires a replica set and source pre/post-images; sharded topology is rejected. PostgreSQL requires an operator-created slot/publication and FULL replica identity. ClickHouse is not general native CDC, and current-state queries require FINAL. Interrupted SQL/ClickHouse snapshots can block for explicit reconciliation. Receipt identities are retained without automatic expiry; the configured lifetime event limit and disk budget can require an explicit increase or a reconciled new generation. Comparisons require quiescent writes and enforce row/byte limits.

Use the per-connector runbooks and the final tested capability matrix before choosing a path. Do not infer universal cross-database compatibility from the presence of a connector.
