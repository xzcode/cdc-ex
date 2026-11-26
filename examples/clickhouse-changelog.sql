-- Explicit review template; CDC EX does not execute this SQL.
-- application must already be an Atomic database.
CREATE TABLE application.order_changes
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

-- Producer contract: immutable, contiguous, commit-visible sequence numbers.
-- startSequence="0" means the first event must be sequence 1.
-- Operations: SNAPSHOT, INSERT, UPDATE, DELETE. UPDATE needs a full before-image.
-- key_json example: {"id":1}
-- after_json example: {"id":1,"name":"example","amount":12.34}
-- Retain every event until the consumer checkpoint has advanced past it.
