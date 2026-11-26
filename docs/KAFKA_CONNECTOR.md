# Kafka connector

Kafka is implemented as an optional source and sink. Direct database synchronization does not require a broker.

The current contract, endpoint options, CDC EX version-1 wire envelope, security settings, checkpoint semantics and real-service fixtures are maintained in **[M5K_KAFKA.md](M5K_KAFKA.md)**. This file replaces the earlier design-only specification so that planned formats cannot be mistaken for implemented features.

Implemented paths include database→Kafka, Kafka→database and Kafka→Kafka. Source groups are task-exclusive; offsets are durable and validated against retention. The target uses keyed acknowledged publication and producer idempotence, while process restart retains at-least-once semantics. Ordering is partition-local. TLS and SASL_SSL PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 have separate real-broker fixtures.

Only CDC EX envelopes are accepted. Plain JSON, Debezium JSON, raw bytes, Avro, Protobuf, Schema Registry and null compaction tombstones require additional decoders and are not currently supported. No topic or consumer group is implicitly reset to repair a blocked task.

See [acceptance](ACCEPTANCE.md) for the SQL→topic A→topic B→SQL pipeline, broker lifecycle tests and transport gate. [Examples](../examples/tasks) include SQL→Kafka and Kafka→PostgreSQL configurations; examples alone are not evidence of a tested pairing.
