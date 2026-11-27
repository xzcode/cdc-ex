# Optional management database (post-rc.1)

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

Existing SQLite instances continue unchanged. Automatic SQLite-to-MySQL/MongoDB migration, switching external backends, distributed locks and HA are **not** provided in this change; startup refuses a backend switch on a directory containing existing state. Do not copy the same bound directory to another running process or share one remote management database across nodes. Local journals remain authoritative for CDC delivery. If a control database is unavailable, management operations may fail; already-running workers have independent local delivery state. Verify task state after recovery.

Stop tasks and the server before taking coordinated backups of both the external database and the complete local data directory. Preserve the same master key separately. Restore them together to one owner. Encryption protects the connection/task JSON body, not the journal payload or all metadata. Audit retention remains bounded (last 100 exposed, up to 10,000 stored).

This change does not itself establish production-scale concurrency. The published rc.1 release remains unchanged.
