# V1 release acceptance

The authority for each published release-candidate artifact is its `*-build.json`, which records the exact source commit, workflow run and tested binary/SBOM hashes. The release page links that successful V1 CI run. A local test from an earlier snapshot is useful evidence but does not replace the final commit gate.

## Reproducible gate

V1 CI builds the Vue UI and executable JAR once under Node 24/JDK 21, runs Java unit tests, and generates a CycloneDX 1.6 SBOM with plugin 2.9.1. Every runtime integration case downloads the same immutable `tested-distribution` artifact. Real Kafka JUnit transport/lifecycle tests additionally build their connector tests against that same source commit.

| Acceptance family | Exact service fixture | Required checks | Script |
| --- | --- | --- | --- |
| SQL four-way | MySQL 8.4.13 / MariaDB 11.4.13 | all four directions; snapshot, empty snapshot restart, DML, changed key, destination outage/retry | `mysql-smoke.sh` |
| MongoDB native | MongoDB 8.0.15 replica set | snapshot boundary, BSON, exact images, delete, clean/hard restart, target failure and expired boundary | `mongodb-smoke.sh` |
| PostgreSQL native | PostgreSQL 17.7 | exact types, TOAST, snapshots including empty, DML/key change, hard restart, schema drift, slot loss/advance and TRUNCATE block | `postgresql-smoke.sh` |
| ClickHouse modes | ClickHouse 25.8.7.3 | history/current state, stable versions, FINAL/tombstones, snapshot restart rules, cursor ties and changelog gaps | `clickhouse-smoke.sh` |
| Kafka pipeline/lifecycle | Apache Kafka 4.0.0, client 3.9.1; MySQL 8.4.13 | SQL→topic A→topic B→SQL, DML/key change, restart, outage, invalid record, rebalance, retention and identity | `kafka-smoke.sh` |
| Kafka transport | Apache Kafka 4.0.0 | TLS, SASL_SSL PLAIN/SCRAM-256/SCRAM-512; certificate, hostname and authentication negatives | `kafka-security-smoke.sh` |
| Mapped MongoDB paths | MySQL + MongoDB fixtures above | explicit `_id`, ObjectId and exact decimal conversions in both exercised directions | `cross-smoke.sh`, suite `mongodb` |
| Mapped PostgreSQL paths | MySQL + PostgreSQL fixtures above | explicit relational type mapping in both exercised directions | `cross-smoke.sh`, suite `postgresql` |
| Mapped ClickHouse paths | MySQL + ClickHouse fixtures above | MySQL→ClickHouse current state, decimal fidelity, key-change tombstones and restart | `cross-smoke.sh`, suite `clickhouse` |
| Control plane / UI | packaged JAR, real MySQL, actual Chrome | authentication/encryption/redaction/ownership, actual connection/task forms, concurrent tasks, pause/resume, comparison and responsive views | `security-smoke.sh`, `ui-smoke.mjs` |
| Measured recovery workload | MySQL 8.4.13, 512 MiB / 1 CPU container | 1,000 snapshot rows, 5,500 DML events, keyed SHA-256 equality, SIGKILL recovery, schema repair and purged-binlog block | `acceptance-smoke.sh` |
| Docker distribution | pinned Node/Maven/Temurin base digests | full build, non-root/read-only runtime, API and state persistence across replacement | `docker-smoke.sh` |
| Release integrity | tested JAR and exact Git source | version/hash/provenance checks, packed dependency inventory, source archive and modified-binary rejection | `package-release.sh` |

This covers the explicit scripts and their assertions. It does not establish all 36 combinations, multi-day soak behavior, all network partitions, power-failure durability of every storage platform or production throughput.

## Run locally

Build the UI and JAR as described in the root README. Acceptance scripts require Docker, Bash, OpenSSL, Python 3 and Java 21; Kafka JUnit fixtures also require Maven. Each script uses disposable, uniquely named services and credentials and removes only its own resources. Services use bounded memory; run sequentially on a small host.

```bash
JAR=cdc-server/target/cdc-server-0.1.0-rc.2.jar
CDC_TEST_SOURCE_TYPE=MYSQL CDC_TEST_TARGET_TYPE=MARIADB bash scripts/mysql-smoke.sh "$JAR"
bash scripts/mongodb-smoke.sh "$JAR"
bash scripts/postgresql-smoke.sh "$JAR"
bash scripts/clickhouse-smoke.sh "$JAR"
CDC_TEST_KAFKA_BROKER_SUITE=true bash scripts/kafka-smoke.sh "$JAR"
bash scripts/kafka-security-smoke.sh "$JAR"
CDC_CROSS_SUITE=mongodb bash scripts/cross-smoke.sh "$JAR"
CDC_TEST_SECURITY_DATABASE=true CDC_TEST_UI=true \
  CDC_UI_CHROMIUM=/path/to/chrome bash scripts/security-smoke.sh "$JAR"
CDC_ACCEPTANCE_REPORT=target/acceptance/acceptance-results.json bash scripts/acceptance-smoke.sh "$JAR"
bash scripts/docker-smoke.sh
```

Repeat the SQL command for the other three source/target choices and cross suite for `postgresql`/`clickhouse`. A service or browser unavailable on the host is a missing gate, not a passing test. Negative CLI cases must actually block; a timeout alone is not success.

## Measured report

The `*-acceptance.json` release asset records elapsed snapshot and DML time, observed events/second, exact database version, hash equality and recovery assertions. Timings include the stated startup/polling overhead on a shared runner. Compare results only with equivalent fixtures; no fixed throughput threshold is invented from one run.

CI artifacts also include JUnit XML and UI screenshots. The release contains the tested JAR, exact source archive, `SHA256SUMS`, CycloneDX SBOM, exact nested JAR/asset inventory, build provenance and this measured report. Verify `sha256sum --check SHA256SUMS` before deployment.

### Optional control stores and container distribution

The control-store integration scripts exercise SQLite, MySQL and MongoDB independently, including real metadata CRUD, credential preservation, key/owner validation, hard-crash recovery, and repeated pause/resume. The MySQL fixture creates five simultaneous CDC tasks; this is a count-cap regression, not a maximum or a scalability guarantee. CI builds the production JAR once and uses its verified hash and source provenance to build the shared runtime stage of the Dockerfile. `scripts/docker-smoke.sh` without `CDC_TEST_DOCKER_DISTRIBUTION` still exercises the normal complete source-build target.

The post-rc.2 migration fixture (`CDC_TEST_CONTROL_MIGRATION=true`, `CDC_TEST_CONTROL_TYPE=SQLITE`) exercises all six directed backend switches using a real MySQL CDC task, compares API metadata and hashes local task files while stopped, rejects unsafe migration requests, and resumes the original generation after each switch. See the corresponding CI job for actual execution evidence.
