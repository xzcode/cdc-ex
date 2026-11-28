# Multi-resource task management and telemetry (development)

The batch workspace reuses existing encrypted connection templates. Discover existing tables, collections or topics using the read-only resource endpoint; review same-name matches or manually specify a different target. The discovery result is bounded (default 500, maximum 2000 per query), and explicitly marks truncation. Refine the query or add manual pairs. This is a response budget, **not a task-count limit**.

Each chosen pair becomes a normal, independent stopped task: one capture worker, one journal and one generation per resource. There is no shared multi-table capture stream or cross-resource atomic delivery. Prepare all target tables/collections/topics and PostgreSQL replication slots/publications separately. Batch creation defaults to identity field mapping in the UI; API clients can provide per-task mappings. Source/client identities must be independent; MySQL/MariaDB use distinct explicit replication IDs, PostgreSQL uses per-task slot/publication, Kafka uses dedicated consumer groups. Source and sink preflight and existing resource ownership/cycle checks still apply at start.

Group labels are persisted as task metadata (up to 80 characters), not part of the capture fingerprint. The task list supports group filtering and current-result selection. Batch start, pause, stop and group assignment report each item's result. A start result of REQUESTED is asynchronous; actual task states show validation or capture outcomes. Inactive tasks are not changed by bulk pause/stop. No task-count quota is introduced.

## API

- `GET /api/connections/{id}/resources?query=batch&limit=500` -> `{"resources":["batch_a","batch_b"],"truncated":false}`. Uses the saved connection and its namespace; discovery is not preflight approval. Supported providers use metadata/catalog/list operations; permissions and connector-specific identifier rules apply.
- `POST /api/tasks/batch` accepts `{"tasks":[...task definitions...]}`. Optional `group`, `sourceOverrides`, `sinkOverrides` fields are supported by both the existing single-create endpoint and batch creation. Resource overrides are restricted to `table` or `topic` plus source-specific replication identifiers. Credentials, host, database, schema and arbitrary connector options cannot be overridden. Every batch is structurally checked before writes; exact retries are unchanged; conflicting identifiers are rejected. Individual persistence failures are reported. Mongo standalone and independent repositories do not provide atomic all-or-none creation; inspect results before retrying.
- `POST /api/tasks/bulk` accepts `{"ids":["batch-1","batch-2"],"action":"start"}`. Actions: start, pause, stop, group (with group string; empty clears). Returns per-task outcome and safe error code.

Example batch task definition:

```json
{"id":"orders-1","name":"Orders","group":"Commerce","sourceConnectionId":"mysql-source","sinkConnectionId":"mysql-target","snapshotMode":"initial","sourceOverrides":{"table":"orders","replicationServerId":"55001"},"sinkOverrides":{"table":"orders_copy"}}
```

## Monitoring

The service samples durable accepted event and acknowledged event **counts** (not sequence IDs) using monotonic elapsed time. `capturedPerSecond`, `deliveredPerSecond`, `rateWindowMillis` are nullable until a sample is available and on inactive workers. Snapshots count as events; duplicates already recorded in the journal do not increase accepted counts. The UI aggregates available active samples and shows coverage. Browser-local history is not persisted; no zero is invented for an unknown sample.

`lastDeliveryDelayMillis` is the age of the most recently acknowledged non-snapshot event, measured at delivery using the connector's supplied source timestamp. It is not current backlog/replication lag, may reflect connector-specific timestamp semantics, and is sensitive to clock differences. Missing/future timestamps are unknown. The last sample time is the delivery progress timestamp. Idle sources do not acquire an ever-growing latency merely because no events arrive. The previous session's delay/rate are not reconstructed from persisted counters. Inactive journal counts can be recovered by a read-only journal query after process restart.

The UI polls as before (10 seconds). Large deployments need external capacity planning and may require a separate metrics backend; this change does not certify production-scale concurrency.
