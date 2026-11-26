# MySQL connector runbook

This file retains the original M1 link, but describes the current MySQL provider and common runtime. Use the [operator runbook](RUNBOOK.md) for JAR/Compose startup, authentication, limits, pause/resume, comparison and backup; use [field mappings](MAPPINGS.md) for heterogeneous destinations. The application is no longer restricted to a MySQL-only CLI.

The source embeds Debezium's MySQL connector and reads row binlogs; the sink uses JDBC transactions. One task selects one existing InnoDB table. The disposable fixture pins `mysql:8.4.13`; actual accepted builds, versions and scenarios are recorded in the [release acceptance report](ACCEPTANCE.md).

## Endpoint and source prerequisites

Set `type=MYSQL`. String options are `host`, `port` (default `3306`), `user`, `password`, `database`, `table` and `sslMode` (default `VERIFY_IDENTITY`). A source also requires `replicationServerId`, an integer string from 1 through 4294967295, distinct from the source server ID and every other replication client on that server. The managed service rejects conflicting active replication-client IDs on the same known source server.

TLS modes are `VERIFY_IDENTITY`, `VERIFY_CA`, `REQUIRED` and `DISABLED`. `REQUIRED` encrypts without verifying server identity. Configure certificate and hostname verification for deployment connections. `DISABLED` is provided for explicitly isolated development fixtures; the application does not modify server TLS configuration.

Configure `log_bin=ON`, `binlog_format=ROW` and `binlog_row_image=FULL`. Grant the source account the read, replication, metadata and snapshot-lock permissions required by the pinned Debezium connector: `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE` and `REPLICATION CLIENT`; environments using table-level snapshot locking also need `LOCK TABLES`. Scope these with the [Debezium MySQL setup instructions](https://debezium.io/documentation/reference/3.2/connectors/mysql.html#setting-up-mysql).

**Both source and sink accounts also require `PROCESS`.** CDC EX reads `information_schema.INNODB_TABLES.TABLE_ID` to distinguish a table generation from a same-named replacement or truncation. MySQL requires `PROCESS` for that metadata. Identity includes `server_uuid` and the InnoDB table ID; namespace/configuration and schema are fingerprinted separately. See the [MySQL 8.4 INNODB_TABLES reference](https://dev.mysql.com/doc/refman/8.4/en/information-schema-innodb-tables-table.html).

Size binlog retention for snapshots, outages and recovery from backup. The tool does not enable logging, grant privileges, purge logs or select a new offset when history is unavailable. A full physical clone preserving server/table identities is outside automatic generation fencing.

## Types, keys and destinations

Supported source/sink columns are signed `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`, fixed `DECIMAL(p,s)`, `CHAR`, `VARCHAR` and text types. Integer display widths are normalized. Unsigned, floating-point, temporal, binary, native JSON, spatial and other types are rejected. Generated columns are rejected. A primary key is required; composite keys are supported.

A destination requires a pre-created InnoDB table, reads/metadata access and insert/update/delete privileges. Secondary unique indexes, user triggers and foreign keys involving the destination are rejected. Metadata visibility must cover these constraints. The task owns destination writes; concurrent external writers are outside its replay contract.

Text primary-key columns require `binary` or a collation ending in `_bin`. Incoming text keys ending in an ASCII space are rejected, as are trailing ASCII spaces in fixed `CHAR` values. Numeric range, decimal precision/scale and declared text widths are checked before binding. Strict SQL mode is enabled; an explicit mapping does not authorize rounding or truncation.

The common mapping layer permits renamed fields and compatible target types. It no longer imposes the old M1 requirement of identical source/target column order. Complete mapped row shapes and key fields must still match the target contract. See the [task examples](../examples/README.md).

## Snapshot and recovery

`initial` uses Debezium's consistent scan and binlog boundary and requires an empty fresh state destination. `no_data` omits initial rows; the operator must prepare a consistent target baseline or deliberately accept future-change semantics.

The whole capture batch is journaled before Debezium records are acknowledged. `AtomicOffsetStore` writes a checksummed, forced checkpoint through atomic replacement and directory synchronization. Preserve `journal.db`, `offsets.dat` and `schema-history.dat` together. Restart requires matching task/resource fingerprints, valid offsets and nonempty history. A completed snapshot can resume before the first live DML event. An incomplete initial snapshot is blocked and needs a new reconciled generation.

Captured-table DDL, unsupported destructive operations, table replacement and incompatible schema changes block processing. No DDL is propagated automatically. Source identity/schema is checked with capture batches; the sink checks its pinned table/schema before each transaction. Target commit precedes the local contiguous acknowledgement, so an ambiguous commit can replay. Primary-key upserts/deletes converge under the documented ownership and constraint rules; delivery is at least once.

Delivered payloads are now compacted into permanent identity/digest receipts. The old M1 statement that all delivered payloads remain forever is obsolete. Receipts do not expire automatically, and the event limit applies across the task lifetime. See [capacity and recovery](RUNBOOK.md#6-monitor-capacity-and-delivery); never delete receipt/checkpoint files to force a fresh start.

## Fixtures

```bash
bash scripts/mysql-smoke.sh cdc-server/target/cdc-server-0.1.0-rc.1.jar
CDC_TEST_SOURCE_TYPE=MYSQL CDC_TEST_TARGET_TYPE=MARIADB \
  bash scripts/mysql-smoke.sh cdc-server/target/cdc-server-0.1.0-rc.1.jar
```

MySQL→MySQL, MySQL→MariaDB, MariaDB→MySQL and MariaDB→MariaDB are separate disposable acceptance cases. Other database pairings have separate cross-system fixtures. Actual executions and remaining gaps are recorded centrally; this runbook does not assert that a particular artifact passed a fixture.

## 简体中文

MySQL 源端要求 ROW/FULL binlog、独立复制客户端 ID 和一致性快照所需权限。源端与目标端还都需要 `PROCESS`，用于读取 InnoDB 表 ID，防止同名重建表沿用旧状态。当前类型范围为有符号整数、固定精度十进制数和字符/文本，不自动支持所有 MySQL 类型。

目标表须预先创建，主键稳定，不得有不受支持的触发器、外键或次级唯一约束。文本主键必须使用二进制排序规则，尾随 ASCII 空格会被拒绝。完成的初始快照可直接续传；中断快照、位点缺失、日志过期或结构变化会阻断，不能靠删状态文件恢复。通用操作请看 [操作手册](RUNBOOK.md)。
