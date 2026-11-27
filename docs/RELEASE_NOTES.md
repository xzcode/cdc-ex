# CDC EX 0.1.0-rc.2

This release candidate delivers the M1–M9 single-node CDC EX implementation with MySQL, MariaDB, MongoDB, PostgreSQL, ClickHouse and optional Kafka endpoints, plus an English/Simplified Chinese management UI.

## Changes since rc.1

- Refined English/Chinese management UI, responsive mobile task cards, connector identity and readable comparison verdicts.
- SQLite (default), MySQL and MongoDB selectable management databases for encrypted connection/task metadata, state and audit; per-task delivery journal and native checkpoints remain local.
- Removed the fixed active-task count cap; no replacement numerical task quota. Actual concurrency depends on resources and safe endpoint ownership. The five-concurrent-task MySQL fixture is a regression case, not a maximum.
- Graceful stop retains in-flight capture batches before acknowledgement when capacity permits; serialized terminal task-state transitions avoid a stale PAUSING/STOPPING state.
- Kafka broker-outage acceptance covers a retained-generation restart if an in-flight metadata call fails, never an offset reset. Docker CI uses the same checksum-verified tested JAR and shared runtime image stage.

## Included

- MySQL and independent MariaDB snapshot/binlog sources and transactional keyed targets.
- MongoDB replica-set fixed-boundary snapshots, exact-image Change Streams and BSON-preserving targets.
- PostgreSQL pgoutput snapshots/streaming and typed transactional targets, with slot ownership/retention and TRUNCATE safeguards.
- ClickHouse event-history and versioned current-state sinks, plus finite snapshots, guaranteed append cursors and explicit producer changelogs.
- Kafka CDC EX envelope source/sink, durable per-partition offsets, identity fencing, keyed publication, TLS and SASL_SSL PLAIN/SCRAM support.
- SQLite durable capture/delivery separation, atomic checksummed native checkpoints, retained replay receipts, bounded resources and repair/retry controls.
- Explicit field/key mapping and exact conversions, schema/identity fences, DDL preview and bounded quiescent data comparison.
- Authenticated management API, encrypted connection/task configuration, safe diagnostics, audit, process locks, cycle checks and managed concurrent tasks without a fixed task-count cap.
- Vue/TypeScript UI, executable Java 21 JAR, non-root read-only Docker distribution and persistent-volume Compose setup.
- Real-service integration/failure fixtures, actual-browser task acceptance, measured recovery workload, release provenance, SBOM and checksum validation.

## Acceptance and assets

Publication requires every job in the V1 CI workflow to pass for this exact commit. The tested JAR is reused across runtime integration jobs and copied into the release without rebuilding. The source SHA and successful CI link are appended to these notes by the release workflow.

The service fixtures are MySQL 8.4.13, MariaDB 11.4.13, MongoDB 8.0.15, PostgreSQL 17.7, ClickHouse 25.8.7.3 and Apache Kafka 4.0.0; Kafka Java client is 3.9.1. See `docs/ACCEPTANCE.md` for exercised paths. The measured report records 1,000 snapshot rows and 5,500 DML events, keyed content equality, hard-crash recovery, schema repair and expired-binlog blocking. Reported throughput is specific to that shared-runner fixture.

Assets include the executable JAR, exact source archive, `SHA256SUMS`, CycloneDX JSON SBOM, packaged dependency/asset hashes, build provenance and measured acceptance JSON. Validate checksums before running. Docker images can be built from the included source; this release does not claim a separately published container registry image.

## Compatibility and limits

This is a prerelease, with one table/collection/topic per task and no fixed active-task count cap. Delivery is at least once; heterogeneous source transactions are not atomic end to end. SQL types use explicit allowlists. MongoDB requires a replica set and exact images; sharded clusters are rejected. Kafka accepts CDC EX envelopes only. ClickHouse cursor mode is not general native CDC and current-state reads require `FINAL` plus tombstone filtering.

Automatic destructive DDL, arbitrary transforms, distributed HA, multi-master conflict resolution and universal source/target pairing compatibility are outside this release. Comparisons require quiescent data. Retained replay receipts count toward a bounded lifetime budget; defaults are 256 MiB and one million identities, with explicit stopped-task limit changes. Expired positions, resource recreation, incomplete snapshots and invalid events may require explicit reconciliation rather than automatic skipping.

Switching an existing control-store backend requires a separately designed migration. The release's versioned checkpoint fingerprint does not automatically migrate earlier development state. Preserve backups and follow `docs/RUNBOOK.md` when creating a new generation. The deployment encryption key must be kept separately and reused when reopening the control store.

A project license has not yet been selected; the earlier Apache-2.0 proposal is not a license grant. Third-party components retain their own licenses.
