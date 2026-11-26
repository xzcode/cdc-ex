# MongoDB connector

The MongoDB provider is implemented by the MongoDB Java synchronous driver, pinned to 5.4.0. The acceptance fixture uses an isolated `mongo:8.0.15` replica set. This document describes the implementation contract; the final compatibility matrix records which tests actually ran.

## Connection and collection prerequisites

Use endpoint type `MONGODB` and string options `host`, `port` (default `27017`), `database`, and `table` (the collection). A MongoDB `uri` can replace host/port. Optional credentials are `user`, `password`, and `authDatabase` (default `admin`). `replicaSet` can constrain the expected replica-set name. TLS and hostname verification are enabled by default. A private development connection requires both `tls=false` and `allowInsecure=true`; `directConnection=true` is provided for a single-node test replica set.

Both endpoints must already have ordinary, non-capped collections. Source collections require `changeStreamPreAndPostImages.enabled=true`; the tool does not enable this setting on the user's behalf. Source access needs collection reads, metadata reads and Change Streams. Sink access needs reads, metadata reads, and transactional writes. The server must support pre/post-images (MongoDB 6.0+) and replica-set transactions. Standalone, mongos/sharded, capped, time-series and system collections are rejected by this initial provider. Those topologies are not certified by a replica-set test.

The sink requires explicit `documentMode=replace`. This means the task owns the entire mapped destination document: a source field removal removes that field at the destination. `_id` must be explicitly mapped, scalar and non-null; document-valued keys are outside this initial contract. Secondary unique indexes are rejected at preflight; target validators are respected, and a rejected write retains the batch.

## Snapshot, change capture and recovery

`initial` performs a MongoDB snapshot read. The first `find` asks for `readConcern: snapshot`; the returned `atClusterTime` is forced to a checkpoint before the first row is accepted into the journal. Every remaining snapshot batch uses that server cursor. A restart during the snapshot repeats the same immutable timestamp, using deterministic per-key snapshot event identities. A missing or expired timestamp blocks recovery; the provider never silently chooses a new snapshot while retaining the old target.

After all snapshot rows are durable, the checkpoint changes to streaming and the connector opens a collection Change Stream at the saved timestamp. Operations at or before that timestamp are already included in the snapshot. Later operations are journaled in stream order. The stream requests `fullDocument=required` and `fullDocumentBeforeChange=required`; updates use stored exact historical images rather than `updateLookup`, which can observe a later document version. Missing images stop capture.

The opaque resume token advances only after the whole corresponding batch is durably accepted. Empty stream batches can persist their post-batch high-water token. Checkpoint replacement uses a forced temporary file, atomic rename, and directory sync on Linux. Restart uses `resumeAfter` with the saved token. Expired oplog history, lost required images, a missing/corrupt checkpoint, source UUID replacement, schema events, or a changed collection configuration block instead of resetting to the newest position.

`no_data` establishes and persists a snapshot timestamp but emits no initial documents. It then captures only later changes. It is intended for an already prepared destination with an explicitly chosen baseline.

Destination writes run in ordered MongoDB transactions with majority write concern and journaling. A batch checkpoint advances only after confirmed commit. An ambiguous commit is retried through idempotent `_id` replacement and deletion. MongoDB `_id` is immutable, so source key replacement is represented by delete plus insert. An explicitly mapped upstream key change is applied as delete of the previous mapped `_id` followed by replacement of the new one in the same target transaction.

## BSON contract and mapping

Events use canonical Extended JSON for BSON scalars. ObjectId, signed integer widths, Decimal128, doubles, binary subtype/content, date, timestamp, regular expression, null, arrays and nested documents retain their explicit representations through the journal. Missing fields remain absent. JavaScript BSON types and dollar-prefixed field names are rejected; the latter cannot be represented unambiguously alongside Extended JSON type tags.

Native MongoDB-to-MongoDB mapping can retain these tagged values. Relational mappings must explicitly convert a BSON scalar into the destination type; an ObjectId is not automatically converted to a string and a nested document is not silently flattened. Decimal conversion must be exact, and values outside signed 64-bit or Decimal128 range fail instead of rounding. A mapped SQL Decimal becomes BSON Decimal128; SQL integer values use signed 64-bit storage after journal decoding.

`readRows` provides a bounded validation scan in the same encoding. A cross-system comparison only has a stable interpretation when the source is quiescent and the task has no outstanding journal events; it is not a distributed transaction across source and sink.

## Running the acceptance fixture

```bash
mvn -B -ntp clean verify
bash scripts/mongodb-smoke.sh
```

The script owns only its disposable container and temporary directory. It checks the initial snapshot, BSON scalar round-trip, exact field updates/removals, insert/delete, changes while stopped, graceful restart, a hard JVM kill, retained writes during temporary target unavailability, and failure on an explicitly expired start boundary. It prints the actual MongoDB server version. No credentials or event payloads are included in ordinary application logs.

The fixture does not certify multi-shard ordering, replica-set failover, deployments with arbitrary validators, every BSON type, every source/sink pairing, or snapshots longer than the configured history window. Operators must size both oplog retention and the snapshot history window for the task's worst interruption and initial scan duration.

## Primary references

- [MongoDB snapshot read concern and atClusterTime](https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/)
- [MongoDB snapshot session wire protocol specification](https://specifications.readthedocs.io/en/latest/sessions/snapshot-sessions/)
- [MongoDB Change Streams and exact pre/post-images](https://www.mongodb.com/docs/manual/changestreams/)
- [MongoDB server change-stream image semantics](https://github.com/mongodb/mongo/blob/master/docs/change_streams.md)
- [Java driver change streams](https://www.mongodb.com/docs/drivers/java/sync/current/logging-monitoring/change-streams/)
