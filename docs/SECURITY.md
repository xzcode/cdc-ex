# Security and deployment boundaries

CDC EX is a single-node service for an administrator who is authorized to read the configured sources and write the configured destinations. The API can start data replication. Protect access to it with the same care as database administration access.

## Server configuration

The management server uses `CDC_MODE=server`. Configure:

| Variable | Purpose |
| --- | --- |
| `CDC_DATA_DIR` | Private persistent directory containing encrypted control metadata and sensitive task state |
| `CDC_ADMIN_TOKEN` | Administrative bearer token, at least 32 characters; generate a random value |
| `CDC_MASTER_KEY` | Base64 encoding of exactly 32 random bytes, used to encrypt connection/task configuration |
| `CDC_MASTER_KEY_FILE` | File containing the encoded deployment key; use instead of the environment variable when supported by the deployment |
| `CDC_PORT` | Management port, default 8080 |
| `CDC_BIND` | Listener address, default 127.0.0.1 |
| `CDC_ALLOW_REMOTE` | Explicit opt-in required for a non-loopback bind; default false |

The default bind is loopback. For remote administration, use a TLS reverse proxy with an explicit network access policy and retain API bearer authentication. An opt-in to bind remotely does not configure TLS or make the service suitable for unrestricted public access. The service does not supply a multi-user permission model, federated login or distributed ownership.

Generate credentials outside source control. For example, `openssl rand -hex 32` generates an administrative token, and `openssl rand -base64 32` generates a deployment key. Keep both values out of URLs, command histories, issue descriptions, screenshots and checked-in configuration. Give key files access only to the service account. Mounted secret files are preferable to values embedded directly in a Docker Compose file.

The data directory is exclusive to one running server and is restricted to the service account on POSIX systems. Each task also has an ownership lock. Do not run two servers against the same directory, copy an active task's checkpoint files into another running task, or put the directory on a filesystem that cannot provide the required SQLite locking and atomic rename semantics. Stop the service before making a portable backup of the whole directory.

## API authentication and browser access

`GET /api/health` returns nonsensitive liveness information without authentication. All control, connection, task, comparison and audit APIs require `Authorization: Bearer <token>`. The public UI assets contain no database credentials. Authentication is checked before returning control data or accepting mutations; an unknown or missing token is rejected.

An administrative token grants access to all configured endpoints and tasks. Use a separate instance when administrators require different trust boundaries. Tokens are not database passwords and are not stored as connection configuration. Changing the administrator token requires a service restart; it does not change replication checkpoints or encrypted records.

The browser must not send this token to third-party origins. No wildcard cross-origin authenticated API access is intended. Keep the UI and API behind the same origin, and apply normal TLS, browser framing and content-security controls at the application/reverse-proxy boundary. Avoid exposing database data in display names or free-form metadata: display names and sanitized operational metadata may be visible to authenticated users and in audit records.

## Encrypted control metadata

The SQLite, MySQL and MongoDB control repositories encrypt the complete endpoint/task JSON body with **AES-256-GCM**. Each record uses a fresh 96-bit nonce and a 128-bit authentication tag. Versioned ciphertext is bound to its record purpose and ID with additional authenticated data. Replacing one connection's ciphertext with another record's ciphertext does not yield a valid decrypted record for the new ID.

A key-check record is verified when the control store opens. A wrong deployment key blocks startup instead of treating existing configuration as empty or silently replacing it. Retain the key with an appropriately protected backup; loss of the key prevents recovery of saved connection credentials. Do not change the key to bypass a failed startup. There is no implicit key replacement or automatic migration to a new key.

Connection-list responses return sanitized configuration and never return stored secret values. Password changes should be supplied through an authenticated connection update while dependent tasks are paused. Driver options that contain passwords, tokens or connection URI credentials require the same treatment as a password field. The source task fingerprint includes structural configuration and resource identity, so changing a resource, mapping or capture policy cannot silently reuse old checkpoints. Credentials embedded in an opaque URI may also affect that fingerprint; prefer separate credential fields when the connector supports them.

Encryption of the control store is not encryption of the entire data directory. Pending journal payloads contain the replicated rows/documents. Checkpoints and schema history expose source names and positions. SQLite receipts retain event identities and payload digests. Use operating-system/disk encryption, backup encryption and restrictive directory ownership to protect this data. Securely deleting individual rows from a database file is not provided.

## Database and broker credentials

Use connector-specific accounts with the privileges needed for the chosen source or sink mode. The application checks connection capabilities and prerequisites; it does not enable binlogs, reconfigure a database, lower TLS policy or grant privileges automatically. Target upserts and deletes modify the selected destination as part of a started task. Destructive DDL and implicit offset resets are not normal task operations.

Database connectivity should use certificate validation in production. Development fixtures explicitly opt into their test security settings and use temporary containers with generated credentials. They are not deployment templates for public services.

Kafka defaults to SSL with hostname verification. SASL is restricted to supported PLAIN/SCRAM mechanisms over TLS. Plaintext requires an explicit development opt-in. A Kafka sink also requires pre-created topics and verified `auto.create.topics.enable=false`; see [Kafka operational notes](M5K_KAFKA.md) for the required ACLs and protocol boundaries.

The real-broker transport fixture `scripts/kafka-security-smoke.sh` passed six targeted tests with Kafka client 3.9.1 and Kafka broker 4.0.0: SSL and SASL_SSL PLAIN/SCRAM-256/SCRAM-512 delivery, plus negative certificate/hostname and password checks. It uses temporary certificates and an isolated loopback broker. This validates the implemented transport settings, not every deployment's ACLs or mutual TLS configuration.

## Failure handling and diagnostics

Normal logs and API errors use sanitized failure categories. They must not echo input JSON, credentials, JDBC/MongoDB URIs with embedded credentials, connection properties, row/document values or raw Kafka message payloads. Inspect sensitive task files only through appropriately authorized operational procedures. The Kafka invalid-record audit contains coordinates and a payload hash rather than the payload itself.

Capture acknowledgments follow local durable journal persistence; sink acknowledgments follow confirmed target application. A rejected event, incompatible schema, missing checkpoint, changed resource or expired source position blocks the task. Repair and resume the same task when possible. A new generation or replacement snapshot requires reconciliation of the target state; removing a state file is not an approved recovery procedure.

Task pause and stop are asynchronous. Wait for PAUSED or STOPPED before editing connection settings, restarting the task or running a comparison. A consistency comparison is bounded and requires the operator to quiesce source writes and drain pending events. It is not a distributed snapshot or a proof that arbitrary concurrent changes matched.

## Security acceptance fixture

Run the actual packaged application with:

```bash
bash scripts/security-smoke.sh cdc-server/target/cdc-server-0.1.0-rc.1.jar

# Also exercise a real MySQL task through the authenticated control API:
CDC_TEST_SECURITY_DATABASE=true bash scripts/security-smoke.sh

# With the UI build and Playwright Chromium installed, create and control a real task through the browser:
CDC_TEST_SECURITY_DATABASE=true CDC_TEST_UI=true bash scripts/security-smoke.sh
```

The fixture uses generated secrets and a private temporary directory. It checks unauthenticated read/write rejection, wrong-token rejection, authenticated connector discovery, connection creation with redacted responses, malformed-body redaction, cross-origin response policy, encrypted persistent configuration, rejection of a second server owner, wrong-key rejection, remote-bind opt-in and successful restart using an owner-readable key file. The optional database branch checks connection preflight and task snapshot/update/pause/resume/stop through the API against an isolated MySQL container. `CDC_TEST_UI=true` additionally creates separate `ui_src`/`ui_dst` fixture databases and invokes the browser acceptance script against the same authenticated application. It removes only the resources that it created.

Passing this fixture establishes those observed behaviors for the tested build. It does not replace an assessment of the deployment's TLS termination, host permissions, database ACLs, network policies or third-party dependencies. Record actual executions and remaining gaps in the release acceptance report.

## 简体中文

管理 API 可启动数据库写入任务，访问权限应按数据库管理权限保护。服务默认仅监听本机；远程访问需要明确启用，并配置 TLS 反向代理及网络访问控制。除不含敏感信息的健康接口外，API 使用管理令牌认证；当前是单管理员、单节点模型。

连接和任务配置以 AES-256-GCM 加密存放在所选 SQLite、MySQL 或 MongoDB 管理数据库，部署密钥由服务外部提供。每条密文使用独立随机 nonce，并绑定记录用途和 ID。使用错误密钥时必须拒绝启动，不能通过删除状态或换密钥来继续运行。请同时妥善备份完整数据目录和部署密钥。

待投递日志中的业务数据没有被该配置加密机制覆盖，仍需文件权限、磁盘加密和备份加密保护。正常日志和 API 错误不应包含密码、带凭据的 URI、原始行数据或消息载荷。任务暂停/停止是异步过程，应等待状态完成后再修改连接或恢复任务。

`scripts/security-smoke.sh` 用真实打包应用检查认证、脱敏、加密持久化、目录独占、错误密钥拒绝及重启。设置 `CDC_TEST_SECURITY_DATABASE=true` 可增加真实 MySQL 的 API 操作验收。该脚本通过并不代表外部 TLS、账户权限和生产网络环境已完成验证。

External control metadata uses the same encryption format. Protect external credentials and configure verified TLS by default. The local deployment binding prevents accidental backend/owner replacement, not distributed fencing; never clone a live task directory or share the external database between installations. Back up external metadata with stopped local journals and the same key. See [CONTROL_DATABASE.md](CONTROL_DATABASE.md).
