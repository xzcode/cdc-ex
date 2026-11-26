# ClickHouse connector: analytical sinks and limited sources

The provider uses the JDK HTTP client with ClickHouse's parameterized HTTP interface. It does not require a JDBC driver or a broker. The integration fixture pins `clickhouse/clickhouse-server:25.8.7.3`; the release acceptance matrix records the observed version and test result. Ordinary ClickHouse tables are not advertised as native CDC sources.

## Connection

Endpoint type is `CLICKHOUSE`. String options are `host`, `port` (TLS default `8443`, HTTP default `8123`), `user` (default `default`), `password`, `database`, and `table`. TLS and hostname verification use the JVM trust configuration. Unencrypted development HTTP requires both `tls=false` and `allowInsecure=true`. Redirects are refused. Database/table/column identifiers are restricted and quoted; cursor values use typed query parameters.

Tables must already exist in an Atomic database with a persistent table UUID. The provider reads system metadata and rejects unsupported table modes before capture. It never creates, truncates or alters an application table. Table UUID, column schema, engine, sorting expression and partition expression are pinned and checked on replay and delivery.

## Sink: versioned current state

Set `mode=current_state` and `exclusiveWriter=true`. The task must exclusively own the target table, including outside writers. A new task requires an empty target even when its source uses `no_data` or cursor mode. This prevents a new journal sequence from being compared against versions from another task generation.

The target stores the complete mapped document in `payload`; it is an event envelope rather than an automatically generated business-column table. Create it explicitly:

```sql
CREATE TABLE analytics.current_items
(
    entity_key String,
    version UInt64,
    is_deleted UInt8,
    event_id String,
    payload String,
    source_position String,
    source_time_ms Int64
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY entity_key
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1;
```

`entity_key` is canonical JSON of the mapped stable key. `payload` retains the mapped post-image JSON, including explicitly tagged BSON values when present. A delete inserts a tombstone; it does not issue a row mutation. A mapped key change inserts an old-key tombstone followed by the new-key row.

Versions come from the durable task journal sequence. The normal row uses `2 * sequence + 1`; an old-key tombstone uses `2 * sequence`. These numbers are assigned before delivery and remain identical across a failed or ambiguous insert retry. Version overflow is rejected. Physical duplicate versions can exist until merges; they carry identical event identity and content under the supported exclusive-writer contract.

Read the logical current state with:

```sql
SELECT entity_key, payload, version
FROM analytics.current_items FINAL
WHERE is_deleted = 0;
```

Plain `SELECT` is not a current-state query. The provider's bounded comparison uses `FINAL` and excludes tombstones. No partition expression is accepted because every version of a logical key must participate in the same replacement scope. Do not run tombstone cleanup or allow external mutations while a task relies on retained versions.

## Sink: append event history

Set `mode=history` and create:

```sql
CREATE TABLE analytics.event_history
(
    event_id String,
    entity_key String,
    operation String,
    before_json Nullable(String),
    after_json Nullable(String),
    source_position String,
    source_time_ms Int64
)
ENGINE = MergeTree
ORDER BY event_id
SETTINGS fsync_after_insert = 1, fsync_part_directory = 1;
```

History preserves every accepted CDC event envelope. Retries may append a duplicate `event_id`; the table is not advertised as exactly once. Consumers that need a distinct event log must deduplicate by event ID with an appropriate query or downstream store. A history table is not a current-state row comparison target.

Both sink modes use synchronous inserts, wait for the complete response and reject ClickHouse exception headers or late error bodies even if the HTTP status is 200. Explicit table `fsync_after_insert=1` and `fsync_part_directory=1` are required so journal acknowledgement does not rely solely on the target's page cache. The connector does not silently enable those table settings. It acknowledges a delivery batch only after the server confirms success; retries retain the same durable versions.

## Source: finite snapshot

Set task `snapshotMode=snapshot`, source `mode=snapshot`, and `keyColumns=id` (comma-separated for composite keys). The source currently accepts ordinary local `MergeTree`, not Distributed, Buffer, replicated, replacing, materialized or external table engines. Supported source columns are integer types, exact decimals, UTF-8 strings, booleans and nullable variants. Binary strings, temporal types, arrays, tuples and maps require a future explicit encoding and are rejected rather than inferred. Invalid UTF-8 fails decoding; it is not replaced with a replacement character.

The connector issues one streaming `SELECT ... ORDER BY <key>`. HTTP result batches are pieces of the same query, not independent paginated SELECTs. ClickHouse holds that query's part snapshot, so concurrent inserts and merges do not change its row set. The provider rejects duplicate/null keys in the scan. After every row is durable in the journal, it marks the finite source complete; the runtime drains the sink and stops. Restart after completion does not take another snapshot.

ClickHouse does not expose that exact completed query snapshot as a durable resume point. An interruption after snapshot emission has begun therefore blocks automatic resume. Recovery requires an explicit new task generation and reconciled/empty destination. The tool does not silently take a new live scan over a partly populated target. This is a finite snapshot feature, not snapshot-to-native-CDC handoff.

## Source: append cursor

Set `snapshotMode=cursor`, source `mode=cursor`, `keyColumns=id`, `cursorColumns=sequence,id`, and `cursorGuarantee=monotonic-visible`.

The producer must guarantee an immutable, unique cursor tuple and visibility in strictly increasing tuple order. Every logical key column must appear in the tuple; cursor columns must be non-null `UInt64`, `Int64` or `String`. The connector selects ordered batches after the saved compound tuple and persists the last tuple only after journal acceptance. The tie-breaker prevents a page boundary from losing rows with the same leading timestamp/sequence. Cursor values are bound with their ClickHouse types.

An append cursor cannot discover arbitrary updates or deletes to previously read rows. It also cannot discover rows that become visible later with a tuple below the saved watermark. The explicit producer guarantee is part of this mode's contract, not a property inferred from a timestamp column. Raw cursor mode cannot infer retention loss from an allowed sequence gap; use a contiguous changelog when missing-record detection is required.

## Source: producer-maintained changelog

Set `snapshotMode=changelog`, source `mode=changelog`, `cursorGuarantee=contiguous-visible` and `startSequence=0`. `startSequence` is the last acknowledged sequence for a new generation; the next event must be exactly one larger.

```sql
CREATE TABLE source.change_log
(
    sequence UInt64,
    event_id String,
    operation String,
    key_json String,
    before_json Nullable(String),
    after_json Nullable(String),
    source_time_ms Int64
)
ENGINE = MergeTree
ORDER BY sequence;
```

The producer owns the correctness of this log. Sequence values must be unique, contiguous, immutable and visible in commit order; records and tombstones must remain available until the task has persisted its cursor. Valid operations are `SNAPSHOT`, `INSERT`, `UPDATE`, and `DELETE`. Keys must be non-null; state writes require a full post-image; updates require a prior image. JSON objects, precision and missing/null distinctions use the CDC EX event contract.

A sequence gap, invalid operation, malformed JSON, missing image, changed schema or changed table UUID blocks capture without advancing the cursor. If a gap is a temporarily delayed record, restoring the missing retained record permits a deliberate retry. If retention has permanently removed it, the operator must reconcile a new generation; no automatic jump to the latest position is offered.

This captures an application-owned change log. It does not infer the source table's changes from ClickHouse mutations.

## Validation and acceptance fixture

```bash
bash scripts/clickhouse-smoke.sh
```

The isolated fixture bounds the ClickHouse container to 768 MiB and one CPU, and configures small background pools. It exercises versioned updates, tombstones, key changes, restart, hard process death, target unavailability with retained retry, schema-drift blocking, changelog gap detection and repair, 130 equal-leading-cursor records spanning a batch boundary, resume with additional ties, finite snapshot completion/restart, unsigned integer precision, and append event history. Unit tests additionally check stable versions on replay, version overflow, exact decimal decoding, typed cursor parameterization, malformed changelog rejection and HTTP 200 late-error handling.

The initial provider does not certify replicated/distributed engines, ClickHouse Cloud SharedMergeTree, external writers, materialized-view side effects, cleanup policies, arbitrary source types, or every heterogeneous pairing. Quiescent bounded comparisons are capped at 100,000 rows and 32 MiB of serialized payload; larger validations require a separate partitioned design.

## Primary references

- [ClickHouse HTTP interface and typed query parameters](https://clickhouse.com/docs/interfaces/http)
- [ReplacingMergeTree versions, delete markers and FINAL](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree)
- [ClickHouse 25.6: single snapshot for SELECT](https://clickhouse.com/blog/clickhouse-release-25-06)
- [MergeTree durability settings](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/MergeTree/MergeTreeSettings.cpp)
- [JSON format precision and UTF-8 settings](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/FormatFactorySettings.h)
