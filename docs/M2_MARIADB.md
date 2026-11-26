# M2 MariaDB connector preview

Independent Debezium MariaDbConnector with MariaDB-specific TLS modes and binlog decoding. Reuses the durable event pipeline and SQL sink implementation; source/target dialects independently selected by CDC_SOURCE_TYPE and CDC_TARGET_TYPE (MYSQL default, MARIADB supported). Do not change endpoint dialect within an existing task generation. Native MariaDB JDBC driver is used. Common TLS modes map DISABLED/REQUIRED/VERIFY_CA/VERIFY_IDENTITY to disable/trust/verify-ca/verify-full for MariaDB. REQUIRED encrypts but does not verify identity. Prefer VERIFY_IDENTITY in production after trust configuration.

## Baseline

MariaDB 11.4.3+ (11.4 LTS smoke fixture; 11.8 candidate). Source requires InnoDB, ROW/FULL binlog, unique replication client ID, log_bin_compress=OFF and binlog_legacy_event_pos=ON. No automatic server configuration changes. Server identity check uses server_uid and server_id, not an immutable database generation; after host replacement, reset/restore or topology change require operator review. Verify account privileges, snapshot locks and log retention.

Source/target signed integer display widths are normalized; column types, nullability, charset/collation, column names/order and primary keys must otherwise match. M1 narrow scalar type subset and target constraint restrictions still apply. Each direction is an independent one-way task, not multi-master. No schema creation, DDL replication, multi-table support, or failover claims.

## Integration gates

CI runs MYSQL->MYSQL, MYSQL->MARIADB, MARIADB->MYSQL, MARIADB->MARIADB with disposable instances. Same-dialect tests may use two databases in one isolated instance, cross-dialect tests use separate containers. All exercise snapshot, insert/update/delete, key change, decimal precision, graceful restart and temporary target failure. Versions and results must be verified from the actual CI runs. Hard-crash recovery and retention gates remain pending. See M1_RUNBOOK.md for existing safety and durability limitations.

Reference: https://debezium.io/documentation/reference/3.2/connectors/mariadb.html
