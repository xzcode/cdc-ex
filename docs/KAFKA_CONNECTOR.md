# Apache Kafka connector (V1, M5K)

Status: planned; not implemented or tested. Kafka is an optional event-stream endpoint, not a required transport or a database log reader. Use org.apache.kafka:kafka-clients, align its version with the embedded Debezium dependencies and verify supported broker versions (3.9/4.x candidates). No Kafka Connect cluster or Schema Registry is mandatory.

## Workflows
- Database -> Kafka: publish CDC snapshot/DML events to an explicit topic, or namespace-to-topic mapping.
- Kafka -> database: decode the CDC EX envelope, compatible Debezium JSON or explicitly mapped plain JSON; route to configured database/table/collection; require a stable key and explicit operation semantics for state writes.
- Kafka -> Kafka: forward mapped messages to an explicit destination topic/cluster. Preserve source metadata; reject self-loop and known graph cycles.
- Raw message forwarding must preserve bytes, null values, keys and headers when raw mode is selected. Extend the current typed row event SPI with a lossless stream record representation before advertising raw support.

## Connection and discovery
Bootstrap servers, client ID, cluster identity, topic and partition metadata, dedicated consumer group per task generation, timeout, batch/poll limits, compression. Support SSL and SASL_SSL with PLAIN/SCRAM initially; plaintext only by explicit opt-in for trusted development networks. Validate credentials, TLS hostname, ACL and topic existence. Never auto-create topics, alter retention or reset shared group offsets. Secrets must remain redacted. Make security support explicit by mechanism.

## Source
Disable auto commit. Read committed transactional records by default (isolation.level=read_committed). Define start mode (earliest retained, latest at task creation, saved offset, explicitly selected partition offsets); persist resolved positions before capture and never silently apply a new start mode after restart. Use auto.offset.reset=none for unexpected gaps/expiry. Store cluster/topic identity (topic ID where available), partition, offset, key, headers, timestamp, payload and stable identity in the journal. Detect recreated topics and out-of-range checkpoints; block and request an explicit recovery decision. Pause/resume with backpressure while maintaining group membership; drain safely at revocation, commit only contiguous durable offsets for owned partitions. Offset committed to broker is next offset, not last processed offset. Reject incompatible or corrupt messages without advancing past them. Retain a local audit of blocked records; do not default to skip/DLQ.

## Sink
Use acknowledgements=all, enable.idempotence=true and compatible retry/in-flight settings. Use a canonical stable entity key to preserve ordering within a partition, and include event ID and source metadata in the envelope/header. For primary key changes emit explicit old-key delete and new-key upsert (different partitions are not globally ordered). Wait for all send acknowledgements in a delivery batch before updating local checkpoint. Broker durability also depends on replication and min.insync.replicas. Local journal replay after a process crash may still duplicate records; consumers should deduplicate by event ID. Never claim end-to-end exactly-once. Default delete representation is a non-null DELETE envelope; optional compaction mode may emit a null-valued tombstone with the stable key and explicitly documented semantics.

## Formats
Initial JSON envelope: schemaVersion, eventId, namespace, operation, key, before, after, sourcePosition, sourceTimestamp. Preserve typed values via explicit encoding and distinguish missing from null. Debezium JSON decoder and plain JSON mapping are separate options; unknown schemas/operations block rather than guess. Avro/Protobuf and Schema Registry integrations are deferred unless explicitly added.

## Acceptance gates
Use a disposable real Kafka broker (KRaft) and isolated topics. Test producer/consumer, source offsets and restart replay, per-partition order and keys, rebalance/partition addition, duplicate event identity, broker disconnect and resume, topic recreation/retention expiry, incompatible message rejection, delete and tombstone semantics, TLS/SASL where advertised. Verify at least database -> Kafka -> database and Kafka -> Kafka paths and their stated limitations. Never run destructive topic or consumer-group operations on shared clusters.

References: https://kafka.apache.org/documentation/#producerconfigs and https://kafka.apache.org/documentation/#consumerconfigs.
