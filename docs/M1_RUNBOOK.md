# M1 implementation preview — MySQL

Not production-ready. The implementation is a single-table, one-way MySQL-to-MySQL CLI runner with Java 21 and Maven. No web server or authentication endpoint is opened yet. It does not advertise other database connectors.

Build: `mvn clean verify`, then `java -jar cdc-server/target/cdc-server-0.1.0-SNAPSHOT.jar`. Without CDC_RUN=true this prints a usage message and exits.

## Configure

CDC_PREFLIGHT_ONLY=true validates prerequisites without initializing connector state. Set CDC_RUN=true; CDC_DATA_DIR to a private persistent directory; CDC_REPLICATION_SERVER_ID to a unique replication client ID. For CDC_SOURCE and CDC_TARGET provide _HOST, _PORT (default 3306), _USER, _PASSWORD, _DATABASE, _TABLE, _SSL_MODE (default VERIFY_IDENTITY; DISABLED only for isolated test networks). Supply secrets through the process environment or secret manager, not the command line or committed files.

CDC_SNAPSHOT_MODE=initial (default) requires an empty pre-created target; no_data requires an operator-provided consistent baseline or deliberate future-only semantics. The source must use ROW binlog, FULL row image and sufficient read/replication/snapshot permissions. Use InnoDB, matching column names/order/types/nullability and matching primary keys, no generated columns, target triggers, secondary unique constraints or foreign keys. M1 supports signed integers, DECIMAL and basic character/text columns only. Temporal, binary, JSON, unsigned and other types are deliberately rejected. Source/target must not be the same physical table. Do not concurrently write to target. Use isolated databases with no business data.

The journal stores accepted event batches before source record acknowledgement, and advances its sink watermark only after target commit. Retains all event IDs and payloads for now; no automatic deletion. Defaults: 256 MiB logical payload and 1 million event cap. These are logical limits, not SQLite physical-size guarantees; provide filesystem headroom. Cap reached blocks capture. Sink errors retry 12 times, then block without advancing the failing batch. Logs from connector dependencies may contain diagnostic metadata; treat logs and data files as sensitive.

Recovery is deliberately conservative: preserve journal, offsets.dat and schema-history.dat together, do not delete files or change the task generation. Missing files block restart. An initial snapshot that has not yet produced a streaming DML event blocks restart even if it might have completed; this avoids silently resnapshotting into a partially populated target. The current file-offset implementation is not a verified crash-consistent snapshot protocol. Transaction grouping across source events, schema-change events, automated journal GC, queue metrics API, and robust task pause/drain are still pending. The target commit and watermark are not atomic across systems; replay requires idempotent target semantics. Do not claim exactly-once.

## Acceptance

Unit tests cover journal replay, batch rollback on capacity overflow and noncontiguous acknowledgements. End-to-end integration must validate MySQL snapshot, row insert/update/delete, key changes, process restart and target disconnect. Current implementation must pass build and isolated database tests before M1 is marked complete.

M1 runtime uses embedded Debezium with ordered processing; dependency versions are pinned for reproducibility and require compatibility/security review before release.
