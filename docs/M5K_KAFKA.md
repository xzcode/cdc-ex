# Kafka source and sink

Kafka is an optional endpoint. Direct database synchronization does not require a broker. The module implements the `ConnectorProvider` management SPI and uses the Kafka Java client version resolved by the root dependency management.

The Kafka-specific acceptance runs on 2026-11-26 used **Kafka client 3.9.1 against a real Kafka 4.0.0 KRaft broker**. Five broker lifecycle/failure cases and six TLS/SASL transport cases passed. Eleven unit tests additionally cover event fidelity, checkpoint integrity and configuration security. The database-to-Kafka-to-database application pipeline is a separate fixture; see the release acceptance report for its result and build identifier.

## Implemented behavior

| Area | Behavior |
| --- | --- |
| Source | One explicit topic, every partition, one dedicated task-owned consumer group |
| Start | `earliest` or `latest` resolved once and persisted before polling; subsequent starts use saved local offsets |
| Consumer | Automatic subscription/rebalance, `read_committed`, no auto commit, no automatic offset reset, no auto-created topics |
| New partitions | Start at their earliest retained offset when assigned; the initial `latest` choice is not applied again |
| Group ownership | All partitions must belong to this task; joining the same group with another worker blocks this task |
| Sink | Existing explicit topic, keyed publication, `acks=all`, producer idempotence, one in-flight request per connection |
| Key changes | An update with a changed key becomes an old-key DELETE followed by a new-key INSERT |
| Format | CDC EX JSON envelope version 1, including exact decimal JSON values and nested typed wrappers |
| Delete | Non-null DELETE envelope; broker tombstones are rejected by the source |
| Ordering | Broker order within each partition; no global ordering or cross-partition transaction atomicity |
| Identity | Cluster ID, topic ID, topic name, partition and offset; topic replacement is rejected |
| Sink repartition | A changed destination partition count is rejected to avoid silently changing entity routing |

Raw bytes forwarding, plain JSON, Debezium JSON, Avro, Protobuf, Schema Registry, compaction tombstone mode and shared multi-worker consumer groups are not implemented. An envelope is required even for Kafka-to-Kafka forwarding. The Kafka transport does not interpret BSON Extended JSON wrappers or coerce database types; explicit mapping belongs to the task mapping layer.

## Endpoint options

```json
{
  "type": "KAFKA",
  "options": {
    "bootstrapServers": "broker.example.com:9093",
    "topic": "orders-cdc",
    "groupId": "cdc-ex-orders-generation-1",
    "startOffset": "earliest",
    "format": "CDC_EX",
    "securityProtocol": "SASL_SSL",
    "saslMechanism": "SCRAM-SHA-512",
    "saslUsername": "cdc-reader",
    "saslPassword": "SET_THROUGH_ENCRYPTED_CONNECTION_STORE"
  }
}
```

`groupId` and `startOffset` apply to sources. The consumer group must be exclusive to this task and must have no committed offsets when a new task generation is created. Kafka task `snapshotMode` is `earliest` or `latest`; it overrides the connection's `startOffset` explicitly. The compatibility alias `initial` resolves to the connection's `startOffset` (default `earliest`). These choices select retained topic replay and never create a database snapshot. Broker retention limits the available history. The resolved start mode is fingerprinted and persisted; restart always uses saved local offsets.

`securityProtocol` defaults to `SSL`. `SASL_SSL` supports PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 through fixed client login modules. `PLAINTEXT` requires the explicit `allowInsecure: "true"` development option. SASL over plaintext is rejected. Hostname verification stays enabled; TLS 1.2 and 1.3 are enabled. Supported certificate options are `sslTruststoreLocation`, `sslTruststorePassword`, `sslTruststoreType`, `sslKeystoreLocation`, `sslKeystorePassword`, `sslKeystoreType` and `sslKeyPassword`. Paths refer to files mounted inside the application process/container.

Only declared options are used. Arbitrary JAAS modules, serializers, interceptors and overridden acknowledgment/offset-reset settings are not accepted as client configuration. `maxPollRecords` defaults to 128 and is limited to 1–1024; `maxMessageBytes` defaults to 1 MiB and is limited to 1–16 MiB. An oversized or incompatible record blocks the source. Payloads and credentials are not included in the blocked-record audit.

### Required broker configuration and ACLs

Create topics and set replication, `min.insync.replicas`, retention and partition count explicitly. The application never creates or deletes a topic or consumer group. The **sink requires `auto.create.topics.enable=false` on every broker**. This prevents a producer metadata request from implicitly recreating a topic that was removed after preflight. Preflight checks the broker setting and rejects a broker whose value cannot be established.

The source requires topic READ/DESCRIBE and group READ/DESCRIBE, including group-offset inspection and commits. The sink requires topic WRITE/DESCRIBE, idempotent production permissions appropriate to the cluster, cluster/node metadata access and broker DESCRIBE_CONFIGS. TLS/SASL must also be checked with the deployment's actual trust chain, identity and ACLs. `kafka-smoke.sh` uses explicit plaintext opt-in; `kafka-security-smoke.sh` separately verifies TLS and all three advertised SASL mechanisms against a real broker. Mutual TLS client certificates and deployment-specific ACL combinations are not certified by those fixtures.

## Wire format

```json
{
  "schemaVersion": 1,
  "event": {
    "id": "stable-source-event-id",
    "database": "sales",
    "table": "orders",
    "operation": "UPDATE",
    "position": "opaque-source-offset",
    "key": {"id": 42},
    "before": {"id": 42, "amount": 12.30},
    "after": {"id": 42, "amount": 14.50},
    "sourceTimeMillis": 1700000000000
  }
}
```

The Kafka record key is the canonical UTF-8 JSON of `[database, table, key]`; the source verifies that key against the envelope. Each entity must have a stable, non-null key. SNAPSHOT, INSERT and UPDATE require an after-image containing that key. DELETE carries a normal envelope and may have no after-image. TRUNCATE, unknown operations, unknown envelope versions, malformed JSON, a mismatched record key and null-valued records are rejected before a journal acknowledgment.

An incoming Kafka record gets its own journal event ID derived from cluster/topic identity plus partition/offset. Repeated delivery of the same broker record has the same ID. Two different broker offsets containing an identical upstream event have different journal IDs. The original event ID and opaque source position remain in the new event's `position`, along with the Kafka coordinates, record timestamp, key and ordered headers. This avoids falsely deduplicating legitimate repeated publications while preserving upstream provenance.

Kafka sink writes include `cdc-ex-event-id` and `cdc-ex-schema-version` headers. Producer idempotence protects a producer session's network retries. A process crash after acknowledged production but before the local sink watermark can publish the same event again. This is **at-least-once delivery**, and downstream state writers must tolerate replay. Kafka records already appended to the topic are not rolled back if another record in the delivery batch fails.

## Checkpoint and failure behavior

For a source batch, the ordering is:

1. Decode and validate every record in the poll batch.
2. Commit the complete normalized batch to the durable local journal.
3. Persist the checksummed `kafka-source.json` using a forced file write, atomic rename and parent-directory force.
4. Commit the **next** contiguous offset for each owned partition to Kafka.

Local offsets are authoritative. A crash between steps 2 and 3 replays records that the journal can deduplicate. A crash after step 3 but before step 4 resumes from local state, even when the group offset is behind. A destination failure does not change this relationship: journal data remains until target application and sink acknowledgment finish.

Before startup and assignment, saved offsets are checked against the retained beginning and end of each partition. Missing checkpoint files on resume, a recreated topic, a different cluster, an out-of-range offset or a changed start policy block the task. No latest-offset fallback occurs. Authentication failures and unrecoverable client errors similarly block capture; resume the same task after repair. A short broker outage may be handled by client retries; a longer outage can require task resume with the preserved state.

`kafka-blocked.json` records invalid-record coordinates, a SHA-256 of the payload, and a sanitized reason. The payload itself remains in Kafka subject to retention. No skip, DLQ or implicit group reset is performed. Reconcile the affected record and downstream state in an explicitly reviewed new task generation when the existing immutable Kafka message cannot be corrected.

The destination has an independent `kafka-sink.json` identity record. Do not remove it or any task checkpoint to make a failed task start. Back up the entire task directory consistently. Checkpoint JSON is an implementation detail, not an operator offset-editing interface.

## Validation fixtures

`scripts/kafka-smoke.sh` starts isolated containers with generated credentials and executes three application tasks:

```text
MySQL source -> Kafka topic A -> Kafka topic B -> MySQL target
```

The script checks snapshot, insert/update/delete, primary-key change, consumer restart, destination failure/retry, broker outage/recovery and an invalid envelope that must leave source offsets unchanged. The fixtures use exact `apache/kafka:4.0.0` and `mysql:8.4.13` image tags; the script also prints the versions actually running. It removes only containers/volumes created by the fixture.

```bash
bash scripts/kafka-smoke.sh cdc-server/target/cdc-server-0.1.0-rc.2.jar

# Include the broker-specific Java integration suite while the disposable broker is alive:
CDC_TEST_KAFKA_BROKER_SUITE=true bash scripts/kafka-smoke.sh
```

`KafkaBrokerTest` is skipped unless `CDC_TEST_KAFKA_BOOTSTRAP` names an explicitly authorized disposable plaintext broker. Each test creates and removes uniquely named topics/groups. It checks actual broker source/sink round trips, checkpoint recovery after a rejected journal batch, partition addition and reassignment, retained-offset expiry, topic recreation, invalid messages, rejection of a second group member, and prevention of a new task fencing an idle owner before its first committed offset. Unit tests separately cover precise event serialization, stable transport identity, key changes, checkpoint regression, valid-JSON checkpoint corruption and secure option enforcement.

The separate transport fixture requires a Java 21 JDK, Maven, Python 3 and OpenSSL. It resolves the official Kafka 4.0.0 broker runtime artifacts from Maven, generates short-lived private test certificates, binds every broker listener to loopback, and removes its broker state, keys and certificates afterwards:

```bash
bash scripts/kafka-security-smoke.sh
```

`KafkaTransportTest` verifies real source/sink round trips over SSL and SASL_SSL with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512. It first establishes that each negative-test topic exists and is accessible with the valid settings, then verifies the expected authentication exception for an untrusted certificate, a wrong hostname and an incorrect SCRAM password. These six cases passed against the stated versions. No mock broker or mocked producer/consumer is used.

Code and fixtures do not establish a compatibility claim by themselves. Record successful executions, resolved Kafka client version and observed broker version in the release acceptance report. No broad Kafka 3.x/4.x or security-mode certification is implied.

## 简体中文

Kafka 是可选的数据源和目标端，数据库直连同步不依赖 Kafka。当前支持一个明确指定的主题、独占消费组，以及 CDC EX v1 JSON 信封；支持数据库写入 Kafka、Kafka 写入数据库和 Kafka 主题转发。原始消息、Debezium JSON、Avro/Protobuf 与共享消费组暂未实现。

消费端先把整个批次写入本地持久化日志，再保存本地下一条 offset，最后提交消费组 offset。重启优先使用本地位点；位点过期、主题重建、状态文件丢失或无效消息都会阻塞，不会自动跳到最新。新增分区从该分区最早保留的位置开始。目标端分区数变化会阻塞，避免静默改变同一实体的分区路由。

发送端启用 `acks=all` 和生产者幂等，但进程崩溃后的应用重放仍可能产生重复消息，整体语义是至少一次。DELETE 使用非空事件信封，主键变化拆成旧键删除和新键插入。目标端要求所有 broker 关闭自动建主题，并允许读取该配置；主题、权限、保留策略均由操作者预先设置。

默认使用 TLS 并验证主机名；开发环境明文连接必须明确设置 `allowInsecure=true`。独立的 `kafka-security-smoke.sh` 已在真实 Kafka 4.0.0 上通过 SSL、SASL_SSL PLAIN、SCRAM-SHA-256、SCRAM-SHA-512 的消息收发，以及不可信证书、主机名不匹配和错误密码拒绝检查。实际部署的证书链、ACL 与双向 TLS 仍需按具体环境验证。

## Primary references

- [Kafka consumer lifecycle, offsets and rebalance API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Kafka producer acknowledgment and idempotence configuration](https://kafka.apache.org/39/configuration/producer-configs/)
- [Official Kafka Docker image](https://hub.docker.com/r/apache/kafka)
- [Official single-node Docker configuration](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml)
- [Kafka SASL and SCRAM configuration](https://kafka.apache.org/40/security/authentication-using-sasl/)
