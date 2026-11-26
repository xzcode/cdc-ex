# Connector contract

All capabilities are declared, validated at runtime and exposed to the UI. A capability not implemented or tested is not advertised as supported.

## Interfaces

SourceConnector: describe capabilities, validate config, discover namespaces and types, validate capture prerequisites, start snapshot/stream or cursor reader, emit typed batches, pause/close, return opaque position and health.

SinkConnector: describe capabilities, validate mapping/key and target schema, preview DDL, apply ordered batch with stable event IDs, return committed/failed/ambiguous outcomes, close. No success on partial/unconfirmed writes. Support explicit absent vs null and document patches where applicable.

Connector registry identifies database type, supported modes, tested versions, config schema, operation support and diagnostics. Secrets are resolved at runtime. Drivers use parameterized values and validated identifiers.

## Source requirements

| Source | Prerequisites | Resume |
| --- | --- | --- |
| MySQL | binlog enabled, ROW, FULL row image, unique replication client ID, required read/replication privileges | GTID or file/position plus event offset |
| MariaDB | independent Debezium MariaDB connector; ROW/FULL, version-specific legacy position and compression constraints, read/replication privileges | MariaDB GTID or file/position |
| MongoDB | replica set/sharded cluster, Change Streams access, sufficient oplog retention; preimages if required | resume token |
| PostgreSQL | wal_level=logical, pgoutput publication and exclusive task slot, replication permissions and suitable replica identity | LSN plus connector-specific state |
| ClickHouse | readable deterministic query, explicit stable unique cursor tuple or append-only change table with operation, key, payload and sequence | durable compound cursor |

MySQL/MariaDB must preserve connector schema history. MongoDB updates may be patches; require full post-images when necessary or implement safe patch handling, never assume update lookup corresponds to the exact historic event. PostgreSQL TOAST/unavailable values and replica identity must be handled without overwriting data with nulls. Use independent task IDs/slots/offset namespaces; validate source log retention and alert on lag.

ClickHouse cursor polling requires a monotonically progressing, immutable, unique ordering tuple and guaranteed visibility ordering or an explicit overlap/reconciliation policy. A timestamp alone cannot guarantee loss-free capture. Application-owned changelog mode requires producer guarantees, ordered committed records, retained tombstones, and deterministic replay. It is not native CDC of arbitrary ClickHouse tables.

## Target requirements

- MySQL/MariaDB: JDBC transactions, key-based upsert/delete, preserve unsigned numeric range and decimal precision, explicit schema mapping; treat dialect differences separately.
- PostgreSQL: JDBC transactions, ON CONFLICT with a matching unique constraint, keyed deletes, typed JSON/array/binary/time values.
- MongoDB: BSON types and _id mapping; keyed replace/upsert, safe update patches and delete. Do not overwrite unmapped fields accidentally. No automatic ObjectId coercion without mapping rule.
- ClickHouse: batch inserts via JDBC/client. Two modes: append-only event history (event ID carried, replay duplicates possible without separately validated deduplication), and current-state ReplacingMergeTree(version, is_deleted) with immutable unique ORDER BY identity. Stable persisted monotonic versions per logical key; deterministic retries and tombstones. Current-state queries require FINAL or equivalent latest-version logic; do not promise immediate physical deduplication. Reject unsupported mutable sort keys or implement explicit old-key tombstones. Never use a mutation for every row by default.

## Cross-database mapping

Support explicit key and namespace mapping, scalar conversion with overflow/rounding policy, nested JSON/document strategy, timestamp/time-zone policy, null and missing values. Unknown or lossy mappings fail preflight unless user expressly accepts a defined conversion. Binary, decimal, BSON ObjectId, array, JSON and timezone semantics require round-trip tests. Do not assume all 25 source/sink pairs are automatically compatible.

## References

- https://debezium.io/documentation/reference/stable/connectors/mysql.html
- https://debezium.io/documentation/reference/stable/connectors/mariadb.html
- https://debezium.io/documentation/reference/stable/connectors/mongodb.html
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- https://clickhouse.com/blog/clickhouse-postgresql-change-data-capture-cdc-part-1
