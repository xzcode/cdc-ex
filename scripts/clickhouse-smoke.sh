#!/usr/bin/env bash
set -euo pipefail
jar=$(realpath "${1:-cdc-server/target/cdc-server-$(cat VERSION).jar}")
work=$(mktemp -d)
name="cdcex-clickhouse-$RANDOM-$$"
password=$(openssl rand -hex 18)
pid=''
cleanup() {
  result=$?
  if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ $result -ne 0 ]]; then tail -n 100 "$work/worker.log" 2>/dev/null || true; docker logs --tail 40 "$name" 2>/dev/null || true; fi
  docker rm -fv "$name" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$result"
}
trap cleanup EXIT
cat >"$work/small.xml" <<'XML'
<clickhouse>
  <max_server_memory_usage>536870912</max_server_memory_usage>
  <max_thread_pool_size>128</max_thread_pool_size>
  <background_schedule_pool_size>16</background_schedule_pool_size>
  <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <background_pool_size>8</background_pool_size>
  <background_move_pool_size>2</background_move_pool_size>
  <background_fetches_pool_size>2</background_fetches_pool_size>
  <background_common_pool_size>2</background_common_pool_size>
  <merge_tree>
    <number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
  </merge_tree>
</clickhouse>
XML
chmod 644 "$work/small.xml"
docker run -d --name "$name" --memory=768m --cpus=1 -p 127.0.0.1::8123 \
  -e CLICKHOUSE_USER=cdc -e CLICKHOUSE_PASSWORD="$password" -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --mount "type=bind,source=$work/small.xml,target=/etc/clickhouse-server/config.d/cdc-test.xml,readonly" \
  clickhouse/clickhouse-server:25.8.7.3 >/dev/null
port=$(docker port "$name" 8123/tcp | head -n1 | awk -F: '{print $NF}')
ch() { docker exec "$name" clickhouse-client --user cdc --password "$password" --multiquery --query "$1"; }
put() { printf '%s\n' "$1" | docker exec -i "$name" clickhouse-client --user cdc --password "$password" --query 'INSERT INTO src.changes FORMAT JSONEachRow'; }
ready=false
for i in $(seq 1 90); do
  if ch 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]]
ch 'SELECT version(); CREATE DATABASE src; CREATE DATABASE dst;'
state_columns='entity_key String,version UInt64,is_deleted UInt8,event_id String,payload String,source_position String,source_time_ms Int64'
ch "CREATE TABLE dst.states ($state_columns) ENGINE=ReplacingMergeTree(version,is_deleted) ORDER BY entity_key SETTINGS fsync_after_insert=1,fsync_part_directory=1;
CREATE TABLE dst.cursor_states ($state_columns) ENGINE=ReplacingMergeTree(version,is_deleted) ORDER BY entity_key SETTINGS fsync_after_insert=1,fsync_part_directory=1;
CREATE TABLE dst.snapshot_states ($state_columns) ENGINE=ReplacingMergeTree(version,is_deleted) ORDER BY entity_key SETTINGS fsync_after_insert=1,fsync_part_directory=1;
CREATE TABLE dst.history (event_id String,entity_key String,operation String,before_json Nullable(String),after_json Nullable(String),source_position String,source_time_ms Int64) ENGINE=MergeTree ORDER BY event_id SETTINGS fsync_after_insert=1,fsync_part_directory=1;
CREATE TABLE src.changes (sequence UInt64,event_id String,operation String,key_json String,before_json Nullable(String),after_json Nullable(String),source_time_ms Int64) ENGINE=MergeTree ORDER BY sequence;
CREATE TABLE src.append_items (tick UInt64,id String,value Int64) ENGINE=MergeTree ORDER BY (tick,id);
CREATE TABLE src.snapshot_items (id UInt64,name String,amount Decimal(20,4)) ENGINE=MergeTree ORDER BY id;
INSERT INTO src.append_items SELECT 100,toString(number),number FROM numbers(130);
INSERT INTO src.snapshot_items VALUES (1,'one',123.4500),(18446744073709551615,'large',9876543210123.1234);"
python3 - "$work" "$port" "$password" <<'PY'
import json,pathlib,sys
folder,port,password=sys.argv[1:]
base=dict(host='127.0.0.1',port=port,user='cdc',password=password,tls='false',allowInsecure='true')
for name,mode,table,target,extra in [
 ('changes','changelog','changes','states',dict(cursorGuarantee='contiguous-visible',startSequence='0')),
 ('cursor','cursor','append_items','cursor_states',dict(keyColumns='id',cursorColumns='tick,id',cursorGuarantee='monotonic-visible')),
 ('snapshot','snapshot','snapshot_items','snapshot_states',dict(keyColumns='id')),
 ('history','snapshot','snapshot_items','history',dict(keyColumns='id'))]:
 source=dict(base,database='src',table=table,mode=mode,**extra)
 sink=dict(base,database='dst',table=target,mode='history' if name=='history' else 'current_state',exclusiveWriter='true')
 task=dict(id='clickhouse-'+name,source=dict(type='CLICKHOUSE',options=source),sink=dict(type='CLICKHOUSE',options=sink),snapshotMode=mode,
           mapping=dict(rename={},conversions={},exclude=[]),journalBytes=268435456,journalEvents=1000000,batchSize=128,maxFailures=30)
 pathlib.Path(folder,name+'.json').write_text(json.dumps(task))
PY
start() {
  local task=$1 first_line=1
  if [[ -f "$work/worker.log" ]]; then first_line=$(( $(wc -l <"$work/worker.log") + 1 )); fi
  CDC_RUN=true CDC_TASK_FILE="$work/$task.json" CDC_DATA_DIR="$work/$task-state" java -jar "$jar" >>"$work/worker.log" 2>&1 & pid=$!
  # Preflight must finish before target unavailability is injected on a resumed task.
  for i in $(seq 1 150); do
    if tail -n +"$first_line" "$work/worker.log" | grep -Fq "CDC EX task started: clickhouse-$task"; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then echo 'ClickHouse worker failed during startup' >&2; return 1; fi
    sleep 0.2
  done
  echo 'ClickHouse worker startup timed out' >&2; return 1
}
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=''; }
assert_ch() {
  local query=$1 expected=$2
  for i in $(seq 1 90); do
    if [[ $(ch "$query" 2>/dev/null) == "$expected" ]]; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then echo 'ClickHouse worker exited unexpectedly' >&2; return 1; fi
    sleep 1
  done
  echo 'ClickHouse assertion timed out' >&2
  return 1
}
expect_exit() {
  local success=$1 exited=false
  for i in $(seq 1 60); do
    if ! kill -0 "$pid" 2>/dev/null; then exited=true; break; fi
    sleep 1
  done
  [[ "$exited" == true ]]
  local code=0
  wait "$pid" || code=$?
  pid=''
  if [[ "$success" == true ]]; then [[ $code == 0 ]]; else [[ $code != 0 ]]; fi
}
put '{"sequence":1,"event_id":"one","operation":"INSERT","key_json":"{\"id\":1}","before_json":null,"after_json":"{\"id\":1,\"name\":\"one\"}","source_time_ms":1}'
start changes
assert_ch "SELECT count() FROM dst.states FINAL WHERE is_deleted=0 AND JSONExtractString(payload,'name')='one'" 1
put '{"sequence":2,"event_id":"two","operation":"UPDATE","key_json":"{\"id\":1}","before_json":"{\"id\":1,\"name\":\"one\"}","after_json":"{\"id\":1,\"name\":\"updated\"}","source_time_ms":2}'
put '{"sequence":3,"event_id":"three","operation":"INSERT","key_json":"{\"id\":2}","before_json":null,"after_json":"{\"id\":2,\"name\":\"second\"}","source_time_ms":3}'
put '{"sequence":4,"event_id":"four","operation":"DELETE","key_json":"{\"id\":2}","before_json":"{\"id\":2}","after_json":null,"source_time_ms":4}'
assert_ch "SELECT count() FROM dst.states FINAL WHERE is_deleted=0 AND JSONExtractString(payload,'name')='updated'" 1
put '{"sequence":5,"event_id":"five","operation":"UPDATE","key_json":"{\"id\":3}","before_json":"{\"id\":1,\"name\":\"updated\"}","after_json":"{\"id\":3,\"name\":\"moved\"}","source_time_ms":5}'
assert_ch "SELECT groupArray(JSONExtractInt(payload,'id')) FROM dst.states FINAL WHERE is_deleted=0" '[3]'
stop
put '{"sequence":6,"event_id":"six","operation":"INSERT","key_json":"{\"id\":4}","before_json":null,"after_json":"{\"id\":4,\"name\":\"offline\"}","source_time_ms":6}'
start changes
assert_ch 'SELECT count() FROM dst.states FINAL WHERE is_deleted=0' 2
kill -KILL "$pid"; wait "$pid" 2>/dev/null || true; pid=''
start changes
ch 'RENAME TABLE dst.states TO dst.states_hold'
put '{"sequence":7,"event_id":"seven","operation":"INSERT","key_json":"{\"id\":5}","before_json":null,"after_json":"{\"id\":5,\"name\":\"retained\"}","source_time_ms":7}'
sleep 2
ch 'RENAME TABLE dst.states_hold TO dst.states'
assert_ch 'SELECT count() FROM dst.states FINAL WHERE is_deleted=0' 3
# Schema changes and missing changelog sequence numbers fail without skipping records.
ch 'ALTER TABLE src.changes ADD COLUMN incompatible String'
expect_exit false
ch 'ALTER TABLE src.changes DROP COLUMN incompatible'
start changes
put '{"sequence":9,"event_id":"nine","operation":"INSERT","key_json":"{\"id\":9}","before_json":null,"after_json":"{\"id\":9}","source_time_ms":9}'
expect_exit false
put '{"sequence":8,"event_id":"eight","operation":"INSERT","key_json":"{\"id\":8}","before_json":null,"after_json":"{\"id\":8}","source_time_ms":8}'
start changes
assert_ch 'SELECT count() FROM dst.states FINAL WHERE is_deleted=0' 5
stop
start cursor
assert_ch 'SELECT count() FROM dst.cursor_states FINAL WHERE is_deleted=0' 130
stop
ch "INSERT INTO src.append_items VALUES (100,'z',999),(101,'0',999)"
start cursor
assert_ch 'SELECT count() FROM dst.cursor_states FINAL WHERE is_deleted=0' 131
assert_ch "SELECT JSONExtractInt(payload,'value') FROM dst.cursor_states FINAL WHERE is_deleted=0 AND JSONExtractString(payload,'id')='0'" 999
printf '%s\n' '{"tick":102,"id":"slash\\key\t1","value":1}' | docker exec -i "$name" clickhouse-client --user cdc --password "$password" --query 'INSERT INTO src.append_items FORMAT JSONEachRow'
assert_ch 'SELECT count() FROM dst.cursor_states FINAL WHERE is_deleted=0' 132
stop
printf '%s\n' '{"tick":102,"id":"slash\\key\t2","value":2}' | docker exec -i "$name" clickhouse-client --user cdc --password "$password" --query 'INSERT INTO src.append_items FORMAT JSONEachRow'
start cursor
assert_ch 'SELECT count() FROM dst.cursor_states FINAL WHERE is_deleted=0' 133
stop
start snapshot
expect_exit true
[[ $(ch 'SELECT count() FROM dst.snapshot_states FINAL WHERE is_deleted=0') == 2 ]]
[[ $(ch "SELECT JSONExtractRaw(payload,'id') FROM dst.snapshot_states FINAL WHERE JSONExtractString(payload,'name')='large'") == 18446744073709551615 ]]
start snapshot
expect_exit true
[[ $(ch 'SELECT count() FROM dst.snapshot_states FINAL WHERE is_deleted=0') == 2 ]]
start history
expect_exit true
[[ $(ch 'SELECT count() FROM dst.history') == 2 ]]
echo 'ClickHouse versioned current state, tombstones, key change, restart, retained retry, schema drift, changelog gap recovery, compound-cursor ties, finite snapshot and event history passed.'
