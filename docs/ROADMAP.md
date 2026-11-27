# M0–M9 delivery map

The `0.1.0-rc.2` implementation covers every stage below. Release publication is conditional on the exact commit passing the aggregate V1 CI gate. This map distinguishes implemented deliverables from the evidence used to accept them; it does not certify untested versions or pairings.

| Stage | Implemented deliverable | Acceptance gate |
| --- | --- | --- |
| M0 | Explicit one-way scope, connector contract and storage ordering | Current scope/architecture/runbooks match implementation |
| M1 | Java modules, typed events, SQLite journal, supervisor, MySQL source/sink | Actual snapshot, empty-snapshot restart, DML, key change, retained retry and crash/expiry workload |
| M2 | Independent MariaDB source and JDBC target | All four MySQL/MariaDB directions |
| M3 | Replica-set MongoDB fixed-boundary snapshot, exact images and BSON target | Native fidelity/recovery; explicit SQL/ObjectId/decimal mapping fixtures |
| M4 | PostgreSQL pgoutput, typed values, operator-owned slot/publication | Snapshot, empty snapshot, TOAST, DML, crash recovery, slot loss/advance and TRUNCATE blocking |
| M5 | ClickHouse history/current-state targets and three limited source modes | Durable versions, tombstones, FINAL state, cursor boundaries and changelog gap rejection |
| M5K | Optional Kafka source/sink, strict envelope, offsets and TLS/SASL | SQL→topic A→topic B→SQL, rebalance/replay/expiry/outage and transport negative tests |
| M6 | English/Chinese Vue UI and authenticated management API | Actual browser forms, concurrent tasks, pause/resume and quiescent comparison |
| M7 | Field/key mappings, schema fences, bounded comparisons and repair/retry | Three heterogeneous fixture families and deliberate failure/recovery assertions |
| M8 | Encrypted config, redaction, safe diagnostics, loopback binding, JAR/Docker | API/security negatives, packaged UI, non-root read-only Docker and persistent volume |
| M9 | Reproducible integration gate, bounded measured workload and release pipeline | All jobs pass; tested binary provenance, SBOM, checksums and release artifact validation |

Exact fixtures: MySQL 8.4.13, MariaDB 11.4.13, MongoDB 8.0.15, PostgreSQL 17.7, ClickHouse 25.8.7.3 and Apache Kafka 4.0.0 with Kafka Java client 3.9.1. Future versions require separate acceptance. [ACCEPTANCE.md](ACCEPTANCE.md) lists the scripts and evidence boundaries.

Deferred work includes distributed scheduling/HA, source fan-out reuse, multi-master conflict handling, automatic schema migration, wider SQL/BSON mappings, additional Kafka envelope formats, stronger cross-system transactional delivery and receipt retention with a separately proven safe reclamation protocol.

Post-M9 UI refinement: [UI polish handoff](HANDOFF_UI_POLISH.md). Included in the rc.2 candidate with the original rc.1 tag and assets preserved.

RC.2 management storage: selectable SQLite/MySQL/MongoDB control repositories and removal of the fixed four-active-task gate. See [CONTROL_DATABASE.md](CONTROL_DATABASE.md); the local CDC journal contract is unchanged.

Post-rc.2: offline management repository migration between all three storage backends. Copy and durable verification precede the binding switch; source and per-task journals are retained. See [CONTROL_DATABASE.md](CONTROL_DATABASE.md).
