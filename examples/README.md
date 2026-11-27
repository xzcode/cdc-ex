# Configuration examples

These examples match `0.1.0-rc.2` task/API classes. They contain reserved example hostnames and placeholder credentials. Copy them outside source control, replace every placeholder, review prerequisites, then run preflight. Endpoint option values are strings; task limits are JSON numbers. JSON does not expand environment-variable references.

Read the [operator runbook](../docs/RUNBOOK.md), [mapping contract](../docs/MAPPINGS.md) and [actual acceptance results](../docs/ACCEPTANCE.md). Configuration examples do not certify an unrun database pairing.

## CLI task files

Files under `tasks/` are complete `TaskSpec` documents for `CDC_TASK_FILE`:

| File | Purpose |
| --- | --- |
| [mysql-to-mariadb.json](tasks/mysql-to-mariadb.json) | MySQL snapshot/binlog to compatible MariaDB rows |
| [mysql-to-mongodb.json](tasks/mysql-to-mongodb.json) | SQL id to BSON Int64 `_id`, amount to Decimal128 |
| [mysql-to-kafka.json](tasks/mysql-to-kafka.json) | SQL changes to keyed CDC_EX/1 envelopes |
| [kafka-to-postgresql.json](tasks/kafka-to-postgresql.json) | Retained CDC_EX/1 envelopes to PostgreSQL rows |
| [mysql-to-clickhouse.json](tasks/mysql-to-clickhouse.json) | SQL changes to versioned ClickHouse current-state envelopes |
| [clickhouse-changelog-to-mysql.json](tasks/clickhouse-changelog-to-mysql.json) | Producer-owned contiguous change log to MySQL rows |

The relational row shape is `id BIGINT`, `name VARCHAR(120)`, `amount DECIMAL(18,2)`; [orders-table.sql](orders-table.sql) is an administrator review template. MongoDB receives `_id`, `name`, `amount` with the selected BSON types. Additional source fields need matching target columns or explicit exclusion. Fresh state destinations must satisfy the applicable empty-target requirement.

MySQL source examples use `replicationServerId="5401"`. Give each concurrent reader on a server a different ID, also distinct from the server ID. Kafka sources need a dedicated exclusive group; a fresh task cannot silently reuse existing group commits. Use a separate task ID and state directory for each generation.

TLS is enabled in these examples. Replace certificate/truststore paths with real readable files mounted inside the process/container, and configure JVM/driver trust where applicable. The fixture's plaintext settings are not deployment defaults.

```bash
CDC_RUN=true CDC_PREFLIGHT_ONLY=true \
CDC_TASK_FILE=/protected/orders.json CDC_DATA_DIR=/persistent/orders-generation-1 \
java -jar cdc-server/target/cdc-server-0.1.0-rc.2.jar

CDC_RUN=true \
CDC_TASK_FILE=/protected/orders.json CDC_DATA_DIR=/persistent/orders-generation-1 \
java -jar cdc-server/target/cdc-server-0.1.0-rc.2.jar
```

Keep the same task semantics and state when resuming. Changing mappings, source modes or physical resources requires a reviewed generation and reconciled destination; a new directory alone does not reconcile data.

### Kafka pipeline

The MySQL-to-Kafka and Kafka-to-PostgreSQL files represent independent tasks joined through an explicit pre-created topic. The topic must contain CDC_EX/1 envelopes with matching Kafka record keys, exact numbers and full images; arbitrary JSON, Debezium JSON and raw tombstones are not accepted. SQL updates require full before-images. Broker auto-topic creation must be disabled and the documented metadata/ACL checks must succeed.

The publisher preserves namespace `app.orders`; the PostgreSQL sink routes to database `analytics`, schema `public`, table `orders`. Review retained topic history and schema generations before selecting `earliest`. Publication is at least once and can include replay duplicates.

### ClickHouse schemas

- [clickhouse-current-state.sql](clickhouse-current-state.sql) matches `analytics.orders_current`, including required table fsync settings and the `FINAL` current-state query.
- [clickhouse-changelog.sql](clickhouse-changelog.sql) matches `application.order_changes`. With `startSequence="0"`, sequence 1 must be the next visible event; gaps block. The producer guarantees immutable contiguous commit visibility and retains unread records.

Ordinary ClickHouse mutations are not inferred by a changelog task. An arbitrary business table is not an accepted envelope sink. Apply reviewed DDL explicitly; CDC EX does not execute these templates.

## Management API bodies

API task creation references saved connections; it does not accept a CLI task file:

| File | Route |
| --- | --- |
| [source-mysql.json](api/source-mysql.json) | `POST /api/connections` |
| [target-mariadb.json](api/target-mariadb.json) | `POST /api/connections` |
| [task-mysql-to-mariadb.json](api/task-mysql-to-mariadb.json) | `POST /api/tasks`, after both connections exist |
| [comparison.json](api/comparison.json) | `POST /api/tasks/{id}/compare`, after quiescence and a paused, drained task |
| [limits.json](api/limits.json) | `PUT /api/tasks/{id}/limits`, for an inactive task; zero retains an existing setting |

`sourceQuiescent=true` is an assertion that the operator must actually satisfy, not a command that freezes the source. The runbook provides authenticated request and polling examples.

[server.env.example](server.env.example) lists the protected environment variables consumed by Compose. Generate the real administrative token and deployment key once, retain the key, and protect the file with mode `0600`. Compose supplies the master key through the environment by default; an owner-readable mounted key requires explicit alternative configuration.

## 简体中文

`tasks/` 是 CLI 完整任务配置，`api/` 是管理 API 请求体，两者不能混用。地址、密码及证书路径都需替换；JSON 内环境变量不会自动展开。复制示例后准备好表、集合、topic、权限与 TLS，按真实字段核对映射，并为并发读取分配不同复制客户端 ID。

先预检再启动，恢复时继续使用原任务与原状态目录。ClickHouse 示例要求明确事件表结构，Kafka 示例要求 CDC_EX/1 消息，不能据此推断支持任意表或任意 JSON。比较请求中的静止源断言必须由实际停写和追平满足。
