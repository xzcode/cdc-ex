# CDC EX

[简体中文](docs/README.zh-CN.md) · [Operator runbook](docs/RUNBOOK.md) · [Release candidates](https://github.com/xzcode/cdc-ex/releases)

CDC EX is a self-hosted, one-way change capture and synchronization service. It includes native database capture, optional Kafka endpoints, a bounded durable journal, explicit mappings, and an English/Simplified Chinese management UI.

**Version: 0.1.0-rc.2.** The M1–M9 implementation is present. A release candidate is published only after the complete [V1 CI](https://github.com/xzcode/cdc-ex/actions/workflows/ci.yml) passes for the exact source commit and packaged JAR. See [acceptance scope](docs/ACCEPTANCE.md), [release notes](docs/RELEASE_NOTES.md) and [known limits](docs/V1_SCOPE.md). Tested configurations are not a claim that every database version or every source/target pairing is compatible.

## Connectors

| Endpoint | Source | Target | Detailed contract |
| --- | --- | --- | --- |
| MySQL | Initial snapshot and row binlog | Transactional keyed upsert/delete | [MySQL](docs/M1_RUNBOOK.md) |
| MariaDB | Independent MariaDB connector, snapshot and row binlog | Transactional keyed upsert/delete | [MariaDB](docs/M2_MARIADB.md) |
| MongoDB | Fixed-boundary snapshot and exact-image Change Streams; replica sets only | BSON-preserving keyed replace/upsert/delete | [MongoDB](docs/M3_MONGODB.md) |
| PostgreSQL | Initial snapshot and pgoutput logical WAL | Transactional keyed upsert/delete | [PostgreSQL](docs/M4_POSTGRESQL.md) |
| ClickHouse | Finite snapshot, guaranteed append cursor or producer-maintained changelog | Event history or versioned current state | [ClickHouse](docs/M5_CLICKHOUSE.md) |
| Apache Kafka | Retained CDC EX envelopes, per-partition offsets | Keyed CDC EX envelope publication | [Kafka](docs/M5K_KAFKA.md) |

Kafka is optional. Direct database tasks need no broker. ClickHouse ordinary tables do not provide general row-level native CDC; cursor mode cannot infer arbitrary updates, deletes or late-visible rows below its watermark.

## Start the management UI

Requires Docker with Compose. From a checkout, create deployment secrets once and preserve them with your backup procedure:

```bash
umask 077
if [ ! -e .env ]; then
  printf 'CDC_ADMIN_TOKEN=%s\nCDC_MASTER_KEY=%s\n' \
    "$(openssl rand -hex 32)" "$(openssl rand -base64 32)" > .env
fi
docker compose up -d --build
```

Open **http://127.0.0.1:8080** and enter the admin token from `.env`. Create source and target connections, test their roles, create a task, validate its mapping and start it. Database resources, publications/slots and Kafka topics must already exist. Credentials are encrypted in the control store; the separate deployment key is required to reopen that store. The Compose container uses UID 10001, a read-only root filesystem and a persistent `/data` volume. Database hosts in container connections must be reachable from that container; `127.0.0.1` means the CDC container itself.

For a downloaded release JAR, Java 21 is sufficient. Configure `CDC_MODE=server`, `CDC_DATA_DIR`, `CDC_ADMIN_TOKEN` and either `CDC_MASTER_KEY` or a protected `CDC_MASTER_KEY_FILE`; run `java -jar cdc-ex-0.1.0-rc.2.jar`. Binding defaults to loopback. [RUNBOOK.md](docs/RUNBOOK.md) contains complete JAR, API, TLS, backup and recovery instructions.

## Build and run a single task

Requires JDK 21, Maven 3.9 and Node 24:

```bash
npm --prefix cdc-web ci
npm --prefix cdc-web run build
mvn -B -ntp clean verify
CDC_RUN=true CDC_TASK_FILE=/absolute/path/task.json \
  CDC_DATA_DIR=/absolute/path/task-state \
  java -jar cdc-server/target/cdc-server-0.1.0-rc.2.jar
```

Start with an [example task](examples/tasks), replace its explicit configuration placeholders, and protect the resulting file. `CDC_PREFLIGHT_ONLY=true` validates prerequisites without starting capture. A task captures one table, collection or topic; managed mode has no fixed active task-count cap. Actual concurrency depends on host resources and endpoint ownership.

## Delivery and recovery

Source offsets advance only after the complete normalized batch is durable in SQLite. A separate sink watermark advances after confirmed target application. Restart can replay an ambiguous delivery: state sinks use stable keys, ClickHouse uses durable journal versions, and Kafka/history consumers must tolerate duplicates. This is **at-least-once delivery**. Source transaction atomicity is not preserved across arbitrary sinks or batches.

The journal compacts acknowledged payloads into retained replay receipts. Defaults are a 256 MiB logical budget and one million lifetime event identities; these are not a rolling retention window. Capacity, log expiry, resource recreation, incompatible schemas and invalid data block explicitly. Repair the cause and resume the same generation, or follow the runbook's reconciliation procedure. Do not delete checkpoint files to force a restart.

## Documentation and validation

- [Scope and limitations](docs/V1_SCOPE.md), [architecture](docs/ARCHITECTURE.md), [connector SPI](docs/CONNECTORS.md), [mappings](docs/MAPPINGS.md).
- [Operations](docs/RUNBOOK.md), [security](docs/SECURITY.md), [milestone map](docs/ROADMAP.md).
- [Acceptance matrix and reproducible commands](docs/ACCEPTANCE.md), [release process](docs/HANDOFF_M9.md).
- V1 CI builds the UI and JAR once, then runs SQL four-way tests, native connectors, Kafka transport and recovery, mapped heterogeneous paths, real browser tasks, measured fault recovery, all three management repositories, Docker persistence and release integrity checks.
- Release assets include that tested JAR, exact source archive, SHA-256 checksums, CycloneDX SBOM, packaged dependency inventory, build provenance and measured acceptance report.

## License

A project license has not yet been selected. Apache-2.0 was proposed during design; no grant should be inferred from that proposal. Third-party components retain their own licenses; the release dependency inventory identifies the packaged components.

### Optional management database

CDC EX supports SQLite (default), MySQL or MongoDB for encrypted connection/task definitions, state, identities and audit records. Per-task journals and native checkpoints remain local in every mode. See [Control database configuration](docs/CONTROL_DATABASE.md). Switching an existing deployment requires the explicit offline migration command (post-rc.2). See the release page for the exact validated candidate.
