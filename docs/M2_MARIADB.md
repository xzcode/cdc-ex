# MariaDB connector runbook

MariaDB uses its own embedded Debezium `MariaDbConnector` and native MariaDB JDBC driver. It shares the journal, common mappings and SQL validation with MySQL, but remains a distinct endpoint type. See the [operator runbook](RUNBOOK.md), [mappings](MAPPINGS.md) and shared [SQL target contract](M1_RUNBOOK.md).

The disposable fixture pins `mariadb:11.4.13`. Actual accepted versions, builds and scenarios are recorded in the [release acceptance report](ACCEPTANCE.md); a working preflight does not certify another version or topology.

## Endpoint and binlog prerequisites

Use `type=MARIADB` and string options `host`, `port` (default `3306`), `user`, `password`, `database`, `table` and `sslMode`. Sources also require a unique `replicationServerId`, 1–4294967295, distinct from the server ID and other replication clients on that server.

| Task TLS option | MariaDB mode | Meaning |
| --- | --- | --- |
| `VERIFY_IDENTITY` | `verify-full` | Default; verify certificate and hostname |
| `VERIFY_CA` | `verify-ca` | Verify the certificate authority |
| `REQUIRED` | `trust` | Encrypt without verifying identity |
| `DISABLED` | `disable` | Isolated development use only |

The source requires InnoDB, `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, `log_bin_compress=OFF` and `binlog_legacy_event_pos=ON`. Supply the read, replication, metadata and snapshot-lock privileges required by [Debezium's MariaDB connector](https://debezium.io/documentation/reference/3.2/connectors/mariadb.html). CDC EX does not modify these settings or grant privileges.

Both endpoints also require `PROCESS` to read `information_schema.INNODB_SYS_TABLES.TABLE_ID`; see the [MariaDB metadata reference](https://mariadb.com/docs/server/reference/system-tables/information-schema/information-schema-tables/information-schema-innodb-tables/information-schema-innodb_sys_tables-table). Identity combines `server_uid`, `server_id` and the InnoDB table ID. `server_uid` is not an immutable restored-database generation identifier. Table replacement is fenced; a physical clone preserving all identities remains outside automatic recovery guarantees.

## Scope and recovery

The signed-integer/text/fixed-decimal subset, explicit mappings, exact value checks, binary-collated text-key requirement and target trigger/foreign-key/secondary-unique restrictions are shared with [MySQL](M1_RUNBOOK.md#types-keys-and-destinations). Integer display widths are normalized. A primary key is required. Each task selects one source and one target; opposite-direction examples are independent one-way tasks, not multi-master replication.

`initial` uses a consistent snapshot followed by binlog capture and requires an empty fresh state destination. `no_data` skips initial rows and needs a deliberate baseline. MariaDB-specific positions remain in the dedicated connector's state. Do not change dialect, source mode, mapping or physical resource within a saved generation.

The capture batch is durable before source acknowledgement; target transactions commit before the journal watermark advances. Missing/corrupt offsets, missing schema history, an incomplete initial snapshot, expired logs, selected-table DDL or changed identities block without resetting offsets. A completed initial snapshot can restart before the first DML event. An interrupted snapshot requires a new generation and destination reconciliation.

## Fixtures

```bash
CDC_TEST_SOURCE_TYPE=MARIADB CDC_TEST_TARGET_TYPE=MARIADB \
  bash scripts/mysql-smoke.sh cdc-server/target/cdc-server-0.1.0-rc.1.jar
CDC_TEST_SOURCE_TYPE=MARIADB CDC_TEST_TARGET_TYPE=MYSQL \
  bash scripts/mysql-smoke.sh cdc-server/target/cdc-server-0.1.0-rc.1.jar
```

All four MySQL/MariaDB directions are separate acceptance cases. The [MySQL-to-MariaDB example](../examples/tasks/mysql-to-mariadb.json) illustrates the current task shape. See [M9 handoff](HANDOFF_M9.md) for executed scenarios; this runbook does not claim an unrun fixture passed.

## 简体中文

MariaDB 使用独立的 Debezium MariaDB 连接器及 JDBC 驱动。除 ROW/FULL binlog 外，还要求关闭 binlog 压缩并启用 `binlog_legacy_event_pos`。源端与目标端需具备读取 InnoDB 表 ID 的 `PROCESS` 权限；`server_uid`、`server_id` 与表 ID 用于恢复校验，但保留全部身份的物理克隆不能视为自动安全切换。

类型、映射和目标约束规则与 MySQL SQL 合同一致。位点、结构历史、配置和目标基线必须保持一致；初始快照中断或日志过期后不会自动重置位置。部署、暂停、扩容、比较和备份统一见 [操作手册](RUNBOOK.md)。
