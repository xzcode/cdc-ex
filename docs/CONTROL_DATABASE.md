# Optional management database

CDC EX separates its **control repository** (connections, encrypted task definitions, status, identities, audit) from per-task **delivery journals and native capture checkpoints**. SQLite remains the default management repository; MySQL and MongoDB are optional alternatives. The task journal always uses local SQLite WAL with durable acknowledgements.

The fixed four-active-task check is removed. There is no replacement task-count limit or license quota. Use real CPU, memory, storage and endpoint capacity to size the node. Container memory and PID budgets are configurable (`CDC_CONTAINER_MEMORY`, `CDC_CONTAINER_PIDS`). Destination exclusivity, replication client ID uniqueness, graph-cycle checks, task locks and per-task journal capacity remain.

## Configuration

Choose at startup with `CDC_CONTROL_DB_TYPE=SQLITE|MYSQL|MONGODB`. The normal `CDC_MODE=server`, `CDC_DATA_DIR`, `CDC_MASTER_KEY` (or key file), and `CDC_ADMIN_TOKEN` still apply. Compose forwards these variables from the protected environment. Never commit real values.

MySQL:

```dotenv
CDC_CONTROL_DB_TYPE=MYSQL
CDC_CONTROL_MYSQL_HOST=db.example
CDC_CONTROL_MYSQL_PORT=3306
CDC_CONTROL_MYSQL_DATABASE=cdc_ex_control
CDC_CONTROL_MYSQL_USER=cdc_ex
CDC_CONTROL_MYSQL_PASSWORD=replace-me
CDC_CONTROL_MYSQL_SSL_MODE=VERIFY_IDENTITY
```

Create the database and a dedicated account in advance. For first startup grant CREATE, SELECT, INSERT, UPDATE, DELETE on this database; after initialization CRUD privileges suffice. The server creates only its own `cdc_ex_records`, `cdc_ex_audit`, `cdc_ex_secrets_check` InnoDB tables. Use reliable InnoDB commit durability; configure TLS trust on the JVM. Non-hostname-verifying modes require explicit `CDC_CONTROL_ALLOW_INSECURE=true`. No automatic database creation or credential logging.

MongoDB:

```dotenv
CDC_CONTROL_DB_TYPE=MONGODB
CDC_CONTROL_MONGO_URI=mongodb://user:password@mongo.example:27017/?authSource=admin
CDC_CONTROL_MONGO_DATABASE=cdc_ex_control
CDC_CONTROL_MONGO_TLS=true
```

Use a dedicated database/account (readWrite for that database). The Mongo control store uses majority reads, majority+journal writes, primary reads, a built-in unique document ID, and a kind/id index. A replica set is not required for control metadata, unlike MongoDB CDC source capture. URI credentials must be percent-encoded; use verified TLS and JVM trust. Explicit opt-in is required to disable TLS.

## Binding and recovery

New external deployments require a new persistent local directory and an unused dedicated remote repository. A durable `control-store.binding` file contains the chosen backend and random deployment identity. The remote encrypted owner check includes that identity and the deployment key. Another independently initialized directory or a different key is rejected. After initial binding, an absent owner record is an error, not an invitation to initialize a blank store. Do not delete or edit binding files to force recovery.

Existing SQLite instances continue unchanged. Ordinary startup still refuses a backend switch. The post-rc.2 offline migration command below explicitly copies and verifies management data before changing the binding. Distributed locks and HA remain out of scope. Do not copy the same bound directory to another running process or share one remote management database across nodes. Local journals remain authoritative for CDC delivery. If a control database is unavailable, management operations may fail; already-running workers have independent local delivery state. Verify task state after recovery.

Stop tasks and the server before taking coordinated backups of both the external database and the complete local data directory. Preserve the same master key separately. Restore them together to one owner. Encryption protects the connection/task JSON body, not the journal payload or all metadata. Audit retention remains bounded (last 100 exposed, up to 10,000 stored).

This change does not itself establish production-scale concurrency. The rc.1 tag and its published assets remain unchanged; rc.2 is gated by its own complete CI run.

The integration suite covers a real metadata restart, key/owner checks, pause and resume after a process crash, and resuming a gracefully paused generation without clearing checkpoints. MySQL exercises five simultaneously running tasks to detect any regression of the former count cap; other management backends are tested independently.

## Offline migration (post-rc.2 development)

Supports every direction between SQLite, MySQL and MongoDB. The published rc.2 binary does not include this command. Use the new source build or an explicitly published later version. Same-backend relocation is not supported by this command.

1. Pause all tasks, wait until they are inactive, and stop the server. Keep source log retention sufficient for the outage.
2. Back up the current control repository, full local data directory and master key together. Create an unused dedicated target database/account when targeting MySQL/MongoDB; never reuse a previous migration destination.
3. Keep all existing `CDC_CONTROL_*` and master-key settings for the source. Add destination settings under the separate `CDC_MIGRATE_*` prefix, and run the same JAR once with `CDC_MODE=migrate-control` and `CDC_MIGRATE_CONFIRM=true`. No HTTP listener or CDC workers start.
4. Only after the command exits successfully, configure the destination using normal `CDC_CONTROL_*` variables and start the server. Resume the saved tasks normally.

Example: existing SQLite to MySQL (with verified TLS and JVM trust configured):

```sh
export CDC_MODE=migrate-control
export CDC_MIGRATE_CONFIRM=true
export CDC_MIGRATE_DB_TYPE=MYSQL
export CDC_MIGRATE_MYSQL_HOST=db.example
export CDC_MIGRATE_MYSQL_PORT=3306
export CDC_MIGRATE_MYSQL_DATABASE=cdc_ex_control_new
export CDC_MIGRATE_MYSQL_USER=cdc_ex
# supply CDC_MIGRATE_MYSQL_PASSWORD securely
export CDC_MIGRATE_MYSQL_SSL_MODE=VERIFY_IDENTITY
java -jar cdc-ex.jar
```

The destination options are the ordinary control variables with `CDC_CONTROL_` replaced by `CDC_MIGRATE_`, including explicit TLS opt-in if needed. For MongoDB set `CDC_MIGRATE_DB_TYPE=MONGODB`, `CDC_MIGRATE_MONGO_URI`, `CDC_MIGRATE_MONGO_DATABASE`, and optionally `CDC_MIGRATE_MONGO_TLS`. For SQLite set `CDC_MIGRATE_DB_TYPE=SQLITE`; a new `control-<uuid>.db` is created instead of overwriting any earlier local database.

The command holds the server and existing task directory locks. It refuses active/interrupted task states, unregistered local task directories, invalid keys, occupied targets, and missing confirmation. It preserves IDs, definitions, credentials (re-encrypted with the same key), states, errors, identities, update timestamps and all retained audit entries in order. It verifies exact metadata equality, closes and reopens the destination and verifies again before atomically replacing the local binding. No plaintext export file is created. No journal or checkpoint is copied, rewritten, reset or deleted.

The old repository is retained. A failed migration before the binding switch leaves the original binding unchanged; the partially initialized destination is not activated. Do not delete owner records to retry. Choose another unused destination and review the original repository. If an interruption occurs during the final switch, inspect `control-store.binding` to determine the selected backend. After a successful switch, do not restore the old binding in isolation; the old repository becomes stale as soon as the new server runs. A rollback needs a coordinated backup restore or another explicit migration to a fresh repository. Do not downgrade to an older binary after changing the binding format. This is not online migration, distributed fencing, or key rotation. Large metadata sets need sufficient heap for the in-memory verification snapshot.
