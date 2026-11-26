# Operator runbook

This runbook describes the `0.1.0-rc.1` implementation. Read the [M9 handoff](HANDOFF_M9.md) and release acceptance report for the exact build, database versions and observed test results. An implemented connector or an example file does not certify every database version or source/sink pairing.

CDC EX runs one-way, single-table or single-collection tasks. MySQL, MariaDB, MongoDB and PostgreSQL have native change capture. ClickHouse has explicitly limited source modes. Kafka is an optional endpoint; direct database tasks do not need a broker. The management server permits at most four active tasks and protects task/data-directory ownership within a single process deployment. This is not an HA deployment protocol.

## 1. Build and choose a launch mode

Use a full Java 21 JDK, Maven and the Node.js version required by `cdc-web/package.json` (at least 22.18.0). From the repository root:

```bash
npm --prefix cdc-web ci
npm --prefix cdc-web run build
mvn -B -ntp clean verify
java -jar cdc-server/target/cdc-server-0.1.0-rc.1.jar
```

The last command prints usage and exits unless a launch mode is configured. Building the UI before Maven includes its assets in the executable JAR. A Java runtime is sufficient to run the packaged JAR. The Dockerfile builds the UI and Java modules in separate pinned build stages.

| Mode | Configuration | Intended use |
| --- | --- | --- |
| Single task | `CDC_RUN=true`, `CDC_TASK_FILE`, `CDC_DATA_DIR` | One foreground task from a private JSON file |
| Management service | `CDC_MODE=server`, `CDC_DATA_DIR`, `CDC_ADMIN_TOKEN`, deployment key | Authenticated UI/API and persistent task definitions |
| CLI preflight | Single-task variables plus `CDC_PREFLIGHT_ONLY=true` | Inspect endpoints and mapping without starting capture |

If `CDC_MODE=server` is set, server mode takes precedence over `CDC_RUN`. Preflight does not prove that an existing checkpoint can resume: opening the saved task also checks its fingerprint and connector recovery state.

## 2. Prepare persistent storage and credentials

Choose an absolute directory owned by the service account. On POSIX systems the application creates/restricts the directory to mode `0700` and refuses a symbolic-link data directory. Use `umask 077` when creating task files, environment files and keys. The runtime requires working SQLite locks, durable file synchronization and atomic rename on its storage.

Management configuration:

| Variable | Value |
| --- | --- |
| `CDC_DATA_DIR` | Required persistent service directory |
| `CDC_ADMIN_TOKEN` | Required random bearer token, 32–4096 characters |
| `CDC_MASTER_KEY` | Base64 encoding of exactly 32 random bytes |
| `CDC_MASTER_KEY_FILE` | Alternative file containing that same encoded key; owner-only permissions, normally `0600` |
| `CDC_BIND` | `127.0.0.1` by default |
| `CDC_PORT` | `8080` by default |
| `CDC_ALLOW_REMOTE` | Must be `true` for a non-loopback listener |

Set one deployment-key mechanism. If `CDC_MASTER_KEY_FILE` is present, it takes precedence over `CDC_MASTER_KEY`. The file must be readable by the application account and must not grant group/other permissions. Preserve the same deployment key across restarts; it decrypts the saved connection and task definitions. A wrong key blocks startup. Administrative-token rotation also needs a restart, but does not change the encryption key or task positions.

For a new deployment, generate a protected environment file once:

```bash
umask 077
install -d -m 700 "$HOME/.config/cdc-ex"
cdc_env_file="$HOME/.config/cdc-ex/server.env"
test ! -e "$cdc_env_file" || { printf 'Existing deployment file: retain its key.\n' >&2; exit 1; }
{
  printf 'CDC_ADMIN_TOKEN=%s\n' "$(openssl rand -hex 32)"
  printf 'CDC_MASTER_KEY=%s\n' "$(openssl rand -base64 32)"
} > "$cdc_env_file"
chmod 600 "$cdc_env_file"
```

The control store encrypts complete endpoint/task JSON with AES-256-GCM. Pending journal events, offsets and schema history remain sensitive files; configuration encryption does not encrypt the complete data directory. Protect the filesystem and its backups accordingly. See [security and deployment boundaries](SECURITY.md).

### Start the JAR as a management service

```bash
set -a
. "$HOME/.config/cdc-ex/server.env"
set +a
export CDC_MODE=server
export CDC_DATA_DIR="$HOME/.local/share/cdc-ex"
export CDC_BIND=127.0.0.1
export CDC_PORT=8080
umask 077
java -Duser.timezone=UTC -jar cdc-server/target/cdc-server-0.1.0-rc.1.jar
```

Open `http://127.0.0.1:8080` and supply the administrative token to the UI. `GET /api/health` is public liveness only. It does not assert that a task is healthy or caught up. All connection, task, comparison and audit APIs require bearer authentication.

### Start with Docker Compose

```bash
docker compose --env-file "$HOME/.config/cdc-ex/server.env" up -d --build
docker compose --env-file "$HOME/.config/cdc-ex/server.env" logs --tail=100 cdc-ex
```

The checked-in `compose.yml` supplies `CDC_ADMIN_TOKEN` and `CDC_MASTER_KEY` from the protected environment file. It does **not** mount a key file by default. It uses a named persistent `/data` volume, a read-only root filesystem, a temporary `/tmp`, and container UID/GID `10001:10001`. The bounded tmpfs explicitly permits executable mappings because SQLite and other JNI dependencies extract native libraries there; `nosuid`, `nodev` and the sticky permission mode remain enabled. Its listener is `0.0.0.0` inside the container with explicit remote-bind opt-in; the published host port remains bound to `127.0.0.1`.

When replacing the named volume with a bind mount, prepare its ownership for UID/GID `10001:10001`; do not solve ownership errors with world-writable permissions. Mount TLS trust material at the paths used in the connections. A deployment that chooses `CDC_MASTER_KEY_FILE` must mount its owner-readable key and configure that variable explicitly.

For remote administration, put the host-loopback service behind a TLS reverse proxy and an explicit network access policy. Changing `CDC_BIND` or enabling `CDC_ALLOW_REMOTE` does not configure TLS. `docker compose down` preserves the named volume; `down -v` destroys it and is not a restart or recovery operation.

## 3. Prepare endpoints before creating a task

Create the required database tables, collections, Kafka topics and replication resources explicitly. CDC EX does not grant privileges, enable logging, create a replacement replication slot or execute DDL for you. Use these connector runbooks for permissions, accepted types, TLS and retention:

| Endpoint | Operational contract |
| --- | --- |
| MySQL | [Binlog, InnoDB, table identity and scalar types](M1_RUNBOOK.md) |
| MariaDB | [Dedicated MariaDB connector and binlog prerequisites](M2_MARIADB.md) |
| MongoDB | [Replica-set identity, pre/post-images and BSON](M3_MONGODB.md) |
| PostgreSQL | [Logical slot/publication, replica identity and types](M4_POSTGRESQL.md) |
| ClickHouse | [Versioned envelopes and limited source modes](M5_CLICKHOUSE.md) |
| Kafka | [CDC_EX envelopes, group ownership, TLS and ACLs](M5K_KAFKA.md) |

Both MySQL/MariaDB endpoints need `PROCESS` access for the InnoDB table-ID fence, in addition to their normal read/replication or write privileges. Both MongoDB endpoints need permission to read `replSetGetConfig`. PostgreSQL endpoints need the documented physical-identity read permission. Treat these as connection prerequisites, not optional diagnostics.

A fresh `initial`, ClickHouse `snapshot`, or Kafka `earliest` task requires an empty state destination when the connector can inspect emptiness. ClickHouse `current_state` always requires an empty table for a fresh task, even for a future-only source, plus `exclusiveWriter=true`. Kafka history topics do not provide a comparable row-state emptiness check. The task must own its destination writes; the service rejects concurrent tasks with the same known target identity and detected replication cycles. This does not detect an external application writer or another independent installation.

## 4. Run a single task

Copy one of the [task examples](../examples/README.md) outside source control, replace every placeholder and review its mapping and source mode. Endpoint `options` values are strings. JSON placeholders are literal text: the application does not interpolate environment variables inside the file.

```bash
umask 077
install -d -m 700 "$HOME/.config/cdc-ex/tasks"
cp examples/tasks/mysql-to-mariadb.json "$HOME/.config/cdc-ex/tasks/orders.json"
chmod 600 "$HOME/.config/cdc-ex/tasks/orders.json"
# Edit orders.json with the actual endpoints and secret values before continuing.

export CDC_RUN=true
export CDC_TASK_FILE="$HOME/.config/cdc-ex/tasks/orders.json"
export CDC_DATA_DIR="$HOME/.local/share/cdc-ex-orders"
CDC_PREFLIGHT_ONLY=true java -jar cdc-server/target/cdc-server-0.1.0-rc.1.jar
java -jar cdc-server/target/cdc-server-0.1.0-rc.1.jar
```

Use a different task ID, data directory and replication-client ID for each independent task. Stop the foreground process with `SIGTERM`/Ctrl-C and allow it to close. The shutdown hook waits up to 45 seconds; service/container shutdown budgets must allow orderly closure. A hard kill is a crash-recovery case, not a normal pause.

Retain the same task ID, directory, endpoints, mapping and source mode when resuming. Resource identities and structural configuration are fingerprinted. Explicit password-field rotation is excluded from that fingerprint; changing an opaque credential-bearing URI may change it. CLI failures exit with code `2` and a sanitized failure category and code location.

## 5. Operate through the API

API requests use a different shape from CLI task files. Create connections containing `endpoint`, then create a task referring to their IDs. Request examples are in `examples/api/`. Unknown structured JSON fields are rejected. Endpoint options are connector-specific; adding arbitrary driver options does not enable a feature.

The following helper reads the bearer header from standard input, so the token value is not placed in curl's argument list:

```bash
cdc_url=http://127.0.0.1:8080
cdc_api() {
  printf 'Authorization: Bearer %s\n' "$CDC_ADMIN_TOKEN" |
    curl --fail-with-body --silent --show-error --header @- "$@"
}
```

After replacing placeholders in private copies of the API example files:

```bash
cdc_api -H 'Content-Type: application/json' -X POST \
  --data-binary @/protected/source-mysql.json "$cdc_url/api/connections"
cdc_api -H 'Content-Type: application/json' -X POST \
  --data-binary @/protected/target-mariadb.json "$cdc_url/api/connections"
cdc_api -X POST "$cdc_url/api/connections/mysql-orders-source/test?role=SOURCE"
cdc_api -X POST "$cdc_url/api/connections/mariadb-orders-target/test?role=SINK"
cdc_api -H 'Content-Type: application/json' -X POST \
  --data-binary @/protected/task-mysql-to-mariadb.json "$cdc_url/api/tasks"
cdc_api -X POST "$cdc_url/api/tasks/mysql-to-mariadb/validate"
cdc_api -X POST "$cdc_url/api/tasks/mysql-to-mariadb/start"
cdc_api "$cdc_url/api/tasks/mysql-to-mariadb"
```

| Operation | Route |
| --- | --- |
| Connector capabilities | `GET /api/connectors` |
| Sanitized connection list | `GET /api/connections` |
| Update an idle connection | `PUT /api/connections/{id}` |
| List tasks / inspect one | `GET /api/tasks`, `GET /api/tasks/{id}` |
| Validate prerequisites | `POST /api/tasks/{id}/validate` |
| Start or resume / deliberate retry | `POST /api/tasks/{id}/start`, `POST /api/tasks/{id}/retry` |
| Pause / stop | `POST /api/tasks/{id}/pause`, `POST /api/tasks/{id}/stop` |
| Bounded comparison | `POST /api/tasks/{id}/compare` |
| Update an inactive task’s resource limits | `PUT /api/tasks/{id}/limits` |
| SQL DDL preview | `GET /api/tasks/{id}/ddl` |
| Recent audit records | `GET /api/audit` |

Start, pause and stop return `202` before the operation finishes. After start or retry, poll for `CAPTURING` or a failure state. After pause or stop, wait for `PAUSED` or `STOPPED`, respectively, and inspect any `BLOCKED` result. Runtime states include `STARTING`, `CAPTURING`, `RETRYING`, `STOPPING`, `STOPPED` and `BLOCKED`; management also records `DRAFT`, `PAUSING` and `PAUSED`. There is no separate reliable UI state for each native snapshot phase: `CAPTURING` may include the initial scan.

Pause and stop both retain the task and all offsets. They do not promise to drain the pending queue. To drain, first quiesce source writes and wait for `health.journal.pending` to become zero while the task is still running. A completed finite ClickHouse snapshot drains delivery and stops automatically. After a server restart, interrupted active tasks are marked stopped with `INTERRUPTED_REVIEW_AND_RESUME`; the server does not automatically restart every saved task.

Connection updates require every dependent task to be idle. The body contains the same `id`, a `name` and an `endpoint`; supplied options merge into existing options. Omitted secret fields, and secret fields supplied as an empty string, retain their stored values. Responses list `configuredSecrets` without returning the values. Changing schema, identity, capture policy or mapping requires a reviewed task generation, not a credential-edit workaround.

## 6. Monitor capacity and delivery

| Setting | Default | Supported range / meaning |
| --- | ---: | --- |
| `journalBytes` | `268435456` (256 MiB) | 1 KiB–1 TiB logical events plus replay receipts |
| `journalEvents` | `1000000` | 1–1,000,000,000 unique accepted events over the task lifetime |
| `batchSize` | `128` | 1–10,000 delivery events per batch; size for payload/memory needs |
| `maxFailures` | `12` | 1–1,000 consecutive sink failures before blocking |

Omitted or zero numeric task settings select the defaults. A capture event may be at most 8 MiB of serialized JSON, and a capture batch at most 64 MiB. Those hard event/batch limits are separate from `journalBytes`.

Delivered payloads are compacted into durable receipts containing event identity and payload digest. Receipts are retained indefinitely for safe replay, so **`journalEvents` is a lifetime limit, not queue depth**. Compaction can reduce payload bytes but does not make the lifetime event count decrease. Logical bytes include remaining event payloads and receipt accounting. An additional guard stops append when the SQLite database plus WAL exceeds `3 * journalBytes + 16 MiB`, or free filesystem space falls below 64 MiB. These are guard checks, not a physical disk quota or reserved-space guarantee; allow headroom for SQLite and connector state.

`health.journal.total` is the lifetime unique-event count; `pending` is unacknowledged events; `bytes` is the logical accounting above; `acknowledged` is the durable journal sequence watermark. `retries` reports current consecutive sink failures. `lastProgressMillis` is the last confirmed delivery progress time; `sourceTimeMillis` belongs to the last delivered event and may be zero when the source has no useful timestamp. It is not a universal replication-lag measurement. Health can be absent before a task has run in the current server process.

Capacity applies backpressure while a pending batch can still drain. A full receipt budget, oversized event or exhausted filesystem blocks the task. Preserve its directory, repair the constraint, and resume the same task. Use the supported task-limit update below for a larger budget; do not delete receipts or edit the journal to free space.

### Expand an existing task without resetting it

Pause and wait for an inactive task, or use an already blocked/stopped task. Then update only its resource settings:

```bash
cdc_api -H 'Content-Type: application/json' -X PUT \
  --data-binary @examples/api/limits.json \
  "$cdc_url/api/tasks/mysql-to-mariadb/limits"
cdc_api -X POST "$cdc_url/api/tasks/mysql-to-mariadb/start"
```

The body accepts `journalBytes`, `journalEvents`, `batchSize` and `maxFailures`. Zero or omission retains the current value in this **update** API; it does not reset it to the creation default. The example raises bytes to 512 MiB and lifetime events to two million while retaining batch/retry settings. A task must be inactive, values must remain within the supported ranges, and byte/event limits cannot be lowered below retained usage. The returned task includes a `limits` object. Increasing a budget does not discard receipts, change source offsets or alter the fingerprinted mapping.

For the CLI, stop the process and edit only these four fields in the same task file, preserving its identity and other configuration. Account for actual available memory/disk and source retention before restarting. The 8 MiB event and 64 MiB capture-batch limits are not adjustable through this API.

## 7. Compare rows and review DDL

For a meaningful row comparison:

1. Stop application writes to the source and keep them stopped through the comparison. Also keep the destination under exclusive task ownership.
2. Let capture catch up, and verify the expected changes arrived and `pending=0`. Zero queue depth alone does not prove that a source reader has reached the latest source position.
3. Pause the task and wait for `PAUSED` or otherwise ensure it is inactive with no pending events.
4. Submit an explicit `sourceQuiescent` assertion:

```bash
cdc_api -H 'Content-Type: application/json' -X POST \
  --data-binary @examples/api/comparison.json \
  "$cdc_url/api/tasks/mysql-to-mariadb/compare"
```

`maxRows` defaults to 10,000 and cannot exceed 100,000. Each endpoint read has a 32 MiB serialized-data budget. If a row limit is exceeded the result is incomplete, not a successful partial proof. The result reports counts of missing, extra and different rows, and up to 20 hashed example keys per category; it does not return the row contents. Mapping is applied before comparison. Numeric normalization avoids decimal-scale-only mismatches, while BSON scalar tags and document field order are preserved where document semantics require them.

The endpoint reads are independent and are not a distributed consistent snapshot. The service relies on the operator's quiescence assertion. Kafka streams, ClickHouse event-history targets and unsupported source shapes do not provide general current-row comparison. A ClickHouse current-state target is read with `FINAL` and tombstones excluded; a changelog source does not automatically become a materialized row-state comparison source.

DDL preview returns `executable=false`. It is a suggestion for supported fixed SQL shapes, not a migration executor. Review types, widths, scale, nullability, key collation and constraints against the destination contract, then apply reviewed DDL separately. Native source DDL and unexpected target schema changes block capture/delivery; they are not propagated automatically.

## 8. Recovery and backup

| Situation | Required response |
| --- | --- |
| Temporary target failure | Restore reachability/permissions, then retry the same task; the unacknowledged batch remains |
| Schema/type or mapping failure | Inspect the sanitized category and private source data with appropriate authorization; restore the expected contract or reconcile a new generation |
| Lost/corrupt checkpoint or changed resource identity | Restore a matching complete backup or reconcile a new generation; do not create an empty checkpoint |
| Expired binlog/oplog/WAL/topic history | Recover the retained source history if possible; otherwise prepare a new consistent baseline and generation |
| Incomplete MySQL/MariaDB/PostgreSQL initial snapshot | Automatic restart is blocked; reconcile a new task generation and destination |
| Interrupted ClickHouse finite snapshot | Automatic restart is blocked because the exact SELECT snapshot cannot be reconstructed |
| Interrupted MongoDB initial snapshot | Resume the saved fixed `atClusterTime` if that history still exists; expiration blocks |
| Kafka restart | Resume saved local partition offsets; `earliest`/`latest` is only the initial-generation choice |
| Event identity collision | Preserve evidence and investigate; the same identity must not refer to different content |
| Capacity exhausted | Repair disk space or raise an idle task's supported budgets; receipts and positions remain intact |

Do not delete `journal.db`, offset files, checksums, schema history or snapshot markers to force a task to start. A new generation means a new task ID and dedicated state, plus an explicitly reconciled destination. A new initial scan does not automatically remove stale destination rows that disappeared from the source before that scan. It therefore needs an empty/reconciled destination; ClickHouse versioned targets additionally need a fresh version namespace through their empty-target contract.

Before backup, stop tasks and wait for their workers to close, then stop the server. Back up the complete data directory together with its corresponding deployment key and the external protected configuration used by CLI tasks. Keep source retention long enough to resume from that backup's positions. Restore to a single owner with matching permissions and the same key. Do not combine a journal from one backup with connector offsets from another or start two installations from the same copied task generation. This release has no automated online backup, HA takeover or key-rotation migration.

The delivery guarantee is at least once. Local receipt deduplication and deterministic primary-key writes provide replay convergence within the documented contracts. Kafka publication and ClickHouse history may contain replay duplicates. Source transactions may span capture and delivery batches; cross-table, cross-system and cross-partition transaction atomicity are not promised.

## 简体中文操作要点

先阅读对应连接器的权限、类型和保留期要求，再使用 `examples/tasks/` 中的 JSON 模板。模板里的密码只是占位符，程序不会展开 JSON 中的环境变量。单任务使用 `CDC_RUN=true`、`CDC_TASK_FILE` 和独立的 `CDC_DATA_DIR`；管理服务使用 `CDC_MODE=server`、管理令牌和持久部署密钥。

默认服务只监听本机。Compose 从受保护的环境文件读取管理令牌和主密钥，默认没有挂载密钥文件；容器以 `10001:10001` 运行。完整数据目录与密钥必须一起妥善备份，不能通过换密钥、删位点或清空日志来绕过恢复失败。

暂停和停止是异步操作，也不代表队列已经排空。比较数据时，应先停止源端业务写入，让任务追平并确认 `pending=0`，再暂停并等待完成，最后明确提交 `sourceQuiescent=true`。比较最多 10 万行、每端 32 MiB，只是静止数据上的有界检查，不是跨数据库事务快照。

日志压缩会删除已投递的业务载荷，但永久保留事件去重凭据。默认一百万事件是任务生命周期总量限制，不是积压数量；容量耗尽后保留原任务状态，扩容或修复磁盘再继续。SQL 初始快照和 ClickHouse 有限快照中断后需人工协调新一代任务；MongoDB 可在历史仍保留时从原固定时间戳重读。遇到历史过期、身份变化、字段不兼容或位点损坏，任务停止且不会自动跳到最新位置。

## Implementation references

- [Task configuration and fingerprint](../cdc-runtime/src/main/java/com/cdcex/runtime/TaskSpec.java)
- [Control API](../cdc-server/src/main/java/com/cdcex/server/ControlApi.java)
- [Task ownership and preflight](../cdc-server/src/main/java/com/cdcex/server/TaskRunner.java)
- [Task lifecycle and comparison](../cdc-server/src/main/java/com/cdcex/server/TaskManager.java)
- [Runtime delivery limits](../cdc-runtime/src/main/java/com/cdcex/runtime/TaskRuntime.java)
- [Journal accounting and receipts](../cdc-storage/src/main/java/com/cdcex/storage/SqliteJournal.java)
