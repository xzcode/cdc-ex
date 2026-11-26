# PostgreSQL connector

The `cdc-postgresql` module implements a single-table `pgoutput` source and a transactional PostgreSQL sink. The source embeds Debezium 3.2.7.Final; no Kafka broker is needed. `PostgresProvider` implements the common inspection, capture, sink, and bounded comparison SPI.

Implementation and fixtures are present. Actual database acceptance is recorded in the release acceptance report; the presence of a fixture alone is not a passing test. The fixture pins `postgres:17.7`; runtime preflight currently accepts PostgreSQL major versions 16 and 17. Other major versions are rejected, and PostgreSQL 16 is not certified solely because it passes the version check.

## Connection and task settings

Use `type: POSTGRESQL` and string-valued `options`:

| Option | Meaning |
| --- | --- |
| `host`, `port` | Host and port; default port `5432` |
| `user`, `password` | Runtime credentials; management mode stores connections encrypted |
| `database`, `schema`, `table` | One permanent ordinary table; schema defaults to `public` |
| `sslMode` | `verify-full` by default; also `verify-ca`, `require`, or explicit development `disable` |
| `sslRootCert` | Optional PostgreSQL CA certificate path |
| `allowInsecure` | Must be `true` when `sslMode` is `disable` |
| `slot` | Dedicated, already existing permanent logical slot using `pgoutput`; source only |
| `publication` | Existing publication containing every column of this table without a row filter; source only |

`snapshotMode` is `initial` or `no_data`. `initial` uses Debezium's consistent snapshot followed by its WAL boundary. `no_data` starts from the prepared slot when no durable offset exists, and does not copy existing rows. Neither mode silently creates a replacement snapshot after loss of its state.

The event namespace is `database` plus the unqualified `table`; the provider's fenced physical identity includes the cluster system identifier, database OID, and table OID. PostgreSQL schema is part of endpoint configuration and the source checkpoint fingerprint.

Example task (replace credential placeholders before running):

```json
{
  "id": "pg-orders",
  "source": {
    "type": "POSTGRESQL",
    "options": {
      "host": "source.example", "port": "5432",
      "user": "cdc_reader", "password": "REPLACE",
      "database": "app", "schema": "public", "table": "orders",
      "slot": "cdc_orders", "publication": "cdc_orders_pub",
      "sslMode": "verify-full"
    }
  },
  "sink": {
    "type": "POSTGRESQL",
    "options": {
      "host": "target.example", "port": "5432",
      "user": "cdc_writer", "password": "REPLACE",
      "database": "analytics", "schema": "public", "table": "orders",
      "sslMode": "verify-full"
    }
  },
  "snapshotMode": "initial",
  "mapping": {"rename": {}, "conversions": {}, "exclude": []}
}
```

Run with `CDC_RUN=true`, `CDC_TASK_FILE=/protected/task.json`, and a dedicated `CDC_DATA_DIR`. `CDC_PREFLIGHT_ONLY=true` checks the connection and prerequisites without starting capture.

## Administrator-owned prerequisites

Configure `wal_level=logical`, sufficient replication slots/senders, network/TLS access, table read privileges and a role with replication privileges. The provider never changes PostgreSQL server settings. Create and maintain a dedicated slot/publication and set the source table to `REPLICA IDENTITY FULL` before starting the task. The publication must include insert, update, delete and truncate so unsupported destructive operations are visible and block capture.

For a table named `public.orders`, an administrator can prepare:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION cdc_orders_pub FOR TABLE public.orders;
SELECT pg_create_logical_replication_slot('cdc_orders', 'pgoutput');
```

Run slot creation outside any transaction that has already written data. Each task must own a different slot. Slot removal and retention changes are never automatic.

Both endpoints require permission to read their physical cluster identity with `pg_control_system()`. An administrator can grant `EXECUTE` on that function to the task role without granting superuser. The sink also needs table read, insert, update and delete privileges. It must have a primary key and cannot have user triggers, foreign keys, secondary unique indexes, row-level security, generated/identity columns or table inheritance. Source and sink partitioned/unlogged/temp tables are rejected. These restrictions prevent unsupported write side effects and incomplete snapshot scopes.

## Types and mapping

Supported scalar columns are signed `smallint`, `integer`, `bigint`, fixed-scale `numeric(p,s)`, boolean, text/character types, `date`, time without time zone, timestamp with/without time zone, UUID, JSON/JSONB and bytea. Composite primary keys are supported. Arrays, domains, ranges, floats, unbounded numeric, interval, geometry, custom types, infinity values and years outside 0001–9999 are rejected.

Integers and decimals remain exact JSON numbers through the journal. The target validates integer range, numeric precision/scale, string length and temporal precision before binding parameters. It rejects implicit string-to-number or number-to-boolean conversion; use an explicit task mapping where intended. Target writes cannot silently round a decimal or truncate fractional seconds.

Date/time values are ISO strings whose semantics are enforced by the inspected source/target schema. A timestamp without time zone remains a local date/time; a timestamp with time zone is normalized to UTC. UUID and JSON values remain strings; JSON/JSONB binds with the explicit PostgreSQL type. Binary values have this lossless journal representation:

```json
{"$cdcType": "binary", "value": "BASE64_BYTES"}
```

`REPLICA IDENTITY FULL` supplies before-images and TOAST values. An unavailable-value placeholder blocks capture, including a literal value equal to the reserved placeholder `__CDC_EX_UNAVAILABLE_TOAST__`. It is never written as a replacement value or converted to null.

## Durability, recovery and limits

The complete capture batch enters the durable journal before any record is acknowledged. `cdc-debezium` supplies `AtomicOffsetStore`, which writes a new checkpoint, forces it to storage, atomically replaces the previous checkpoint, then forces the containing directory. Empty, corrupt and legacy serialized checkpoints fail closed. It never treats a truncated file as a fresh source.

On restart, PostgreSQL requires the same fenced source, schema, task/slot/publication settings and a nonempty durable offset. The offset must prove the snapshot completed. An interrupted initial snapshot is deliberately blocked: a new snapshot requires an explicit new generation and target reconciliation. A completed snapshot can restart without waiting for the first live DML event. Missing, busy, incompatible, or lost slots block startup. A retained WAL start beyond the durable position blocks startup, including a dropped and recreated slot that would skip changes. No slot is created or reset by this provider.

Source table identity and schema are checked for every delivered capture batch. Sink identity and schema are checked before each transactional batch. Renames, destructive operations and incompatible type/shape changes are retained as failures until repaired or explicitly recovered. No automatic DDL is executed. A key update is represented by Debezium's ordered delete and insert; the sink also handles an explicit update with a changed old key.

The delivery guarantee is at least once. Target primary-key upserts/deletes make replay converge; cross-task and multi-table transaction atomicity are not promised. A whole capture transaction may span several journal/delivery batches. Credentials, raw row values and database exception messages must not be included in ordinary management diagnostics.

## Fixture

```bash
bash scripts/postgresql-smoke.sh cdc-server/target/cdc-server-0.1.0-SNAPSHOT.jar
```

The script creates its own disposable local PostgreSQL container, random credential, schemas, slot, publication and task directory. It tests snapshot-only restart, exact typed values, an unchanged large TOAST value, insert/update/delete, key changes, offline catch-up, hard process kill/replay, a destination failure, target schema drift/repair, missing-slot rejection and recreated-slot gap rejection. It removes only its own container, volumes and temporary directory. Plaintext is explicitly enabled only for this isolated loopback fixture.

## References

- [Debezium PostgreSQL 3.2 connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html): snapshot/WAL boundary, pgoutput, replica identity and TOAST behavior.
- [PostgreSQL 17 system information functions](https://www.postgresql.org/docs/17/functions-info.html): cluster system identifier and control metadata.
- [PostgreSQL JDBC connection properties](https://jdbc.postgresql.org/documentation/use/): TLS modes, timeouts and typed JDBC access.
- [Debezium 3.2.7 PostgresOffsetContext](https://github.com/debezium/debezium/blob/v3.2.7.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresOffsetContext.java): snapshot completion and persisted LSN fields.
