-- Explicit review template; CDC EX does not execute this SQL.
-- analytics must already be an Atomic database. A new task needs an empty table.
CREATE TABLE analytics.orders_current
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

-- Logical current state:
SELECT entity_key, payload, version
FROM analytics.orders_current FINAL
WHERE is_deleted = 0;
