# Development roadmap

All stages are planned; M0 documents are the initial baseline. No code or real database tests have run yet.

| Stage | Deliverable | Gate |
| --- | --- | --- |
| M0 | Scope, architecture, connector contract, roadmap | reviewed design and explicit limits |
| M1 | Maven modules, event types, connector SPI, SQLite durable journal, supervisor, MySQL source/sink | actual MySQL initial snapshot and live DML; crash/restart and no silent loss |
| M2 | MariaDB independent source/sink, version preflight | real MariaDB DML and restart; MySQL/MariaDB cross-sync |
| M3 | MongoDB source/sink, BSON mappings | replica set change stream, patch/delete, replay and mapped cross-sync |
| M4 | PostgreSQL source/sink, WAL and typed mappings | real pgoutput slot, snapshot, DML and recovery |
| M5 | ClickHouse sink and limited snapshot/cursor/change-table source | batch write, version/tombstone, FINAL comparison, cursor bounds and unsupported mode rejection |
| M6 | Web UI, connection preflight, task wizard, control, monitoring, logs | real tasks through UI with sanitized diagnostics |
| M7 | Mappings, schema drift, data comparison, retry and recovery | type fixtures, supported heterogeneous paths, expired offset and blocked event recovery |
| M8 | Security, i18n, distribution, documented compatibility | authenticated API, persistent volume, JAR/Docker smoke test |
| M9 | V1 integration/performance acceptance and release | tested capability matrix and known limits; no blanket certification |

M1 must implement durable source/sink checkpoint separation before exposing replication. Do not defer basic correctness to a later milestone. Each database stage must add real integration fixtures with isolated credentials and pinned images. Fault-injection tests should target journal commit, source offset flush, target commit and sink watermark boundaries. Avoid unrelated test work; do not advertise untested functionality.

Version baseline candidates: MySQL 8.0/8.4, MariaDB 11.4/11.8, MongoDB 7/8, PostgreSQL 16/17, ClickHouse supported LTS. Select exact dependency and container versions after compatibility review; these are targets, not verified version claims.
