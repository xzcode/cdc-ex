#!/usr/bin/env bash
# Disposable PostgreSQL 17 acceptance. Never points at an existing service.
set -euo pipefail
jar="$(realpath "${1:-cdc-server/target/cdc-server-0.1.0-rc.1.jar}")"
work="$(mktemp -d)"
container="cdc-ex-pg-$$-$RANDOM"
password="$(openssl rand -hex 18)"
pid=""
cleanup() {
  rc=$?
  if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ "$rc" != 0 ]]; then tail -n 100 "$work/runner.log" 2>/dev/null || true; docker logs --tail 30 "$container" 2>/dev/null || true; fi
  docker rm -fv "$container" >/dev/null 2>&1 || true
  rm -rf "$work"
  return "$rc"
}
trap cleanup EXIT
docker run -d --name "$container" --memory=384m --cpus=1 -p 127.0.0.1::5432 -e POSTGRES_PASSWORD="$password" postgres:17.7 -c shared_buffers=32MB -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 >/dev/null
sql() { docker exec -e PGPASSWORD="$password" "$container" psql -h127.0.0.1 -U postgres -d postgres -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
ready=false
for i in $(seq 1 90); do if sql 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi; sleep 1; done
[[ "$ready" == true ]] || { echo 'PostgreSQL not ready'; exit 1; }
echo "PostgreSQL $(sql 'SHOW server_version')"
sql "CREATE SCHEMA src; CREATE SCHEMA dst;
CREATE TABLE src.items(id BIGINT PRIMARY KEY,name TEXT,amount NUMERIC(18,2),payload JSONB,bytes BYTEA,happened TIMESTAMP(6),zoned TIMESTAMP(6) WITH TIME ZONE,day DATE,clock TIME(6),enabled BOOLEAN,uid UUID);
CREATE TABLE dst.items(LIKE src.items INCLUDING ALL);
ALTER TABLE src.items REPLICA IDENTITY FULL;
CREATE PUBLICATION cdc_test_pub FOR TABLE src.items;" >/dev/null
sql "SELECT pg_create_logical_replication_slot('cdc_test_slot','pgoutput');" >/dev/null
sql "INSERT INTO src.items VALUES(1,'initial',123.45,'{\"a\":[1,true,null]}',decode('0001ff0a','hex'),'2024-01-02 03:04:05.123456','2024-01-02 03:04:05.123456+08','2024-02-29','12:34:56.123456',true,'550e8400-e29b-41d4-a716-446655440000');" >/dev/null
port="$(docker port "$container" 5432/tcp | head -1 | sed 's/.*://')"
export CDC_TEST_PG_PORT="$port" CDC_TEST_PG_PASSWORD="$password" CDC_TEST_WORK="$work"
python3 - <<'PY'
import os,json,pathlib
base={'host':'127.0.0.1','port':os.environ['CDC_TEST_PG_PORT'],'user':'postgres','password':os.environ['CDC_TEST_PG_PASSWORD'],'database':'postgres','table':'items','sslMode':'disable','allowInsecure':'true'}
source=dict(base,schema='src',slot='cdc_test_slot',publication='cdc_test_pub')
sink=dict(base,schema='dst')
task={'id':'postgresql-smoke','source':{'type':'POSTGRESQL','options':source},'sink':{'type':'POSTGRESQL','options':sink},'snapshotMode':'initial','mapping':{'rename':{},'conversions':{},'exclude':[]},'journalBytes':268435456,'journalEvents':1000000,'batchSize':128,'maxFailures':12}
p=pathlib.Path(os.environ['CDC_TEST_WORK'])/'task.json';p.write_text(json.dumps(task));p.chmod(0o600)
PY
unset CDC_TEST_PG_PASSWORD
export CDC_RUN=true CDC_DATA_DIR="$work/state" CDC_TASK_FILE="$work/task.json"
CDC_PREFLIGHT_ONLY=true java -jar "$jar"
start() { java -jar "$jar" >>"$work/runner.log" 2>&1 & pid=$!; }
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=""; }
expect_blocked() {
  local status=0
  timeout 30 java -jar "$jar" >>"$work/runner.log" 2>&1 || status=$?
  [[ "$status" == 2 ]] || { echo "Expected a blocked task exit, received $status"; return 1; }
}
assert_sql() {
  local query="$1" expected="$2" actual
  for i in $(seq 1 75); do
    kill -0 "$pid" 2>/dev/null || { echo 'Runner exited'; return 1; }
    actual="$(sql "$query")"
    if [[ "$actual" == "$expected" ]]; then return 0; fi
    sleep 1
  done
  echo "Expected $expected, got $actual"; return 1
}
start
assert_sql "SELECT name||':'||amount FROM dst.items WHERE id=1" "initial:123.45"
assert_sql "SELECT encode(bytes,'hex')||':'||happened::text||':'||clock::text FROM dst.items WHERE id=1" "0001ff0a:2024-01-02 03:04:05.123456:12:34:56.123456"
assert_sql "SELECT (payload->'a'='[1,true,null]'::jsonb)::text||':'||day::text||':'||enabled::text||':'||uid::text FROM dst.items WHERE id=1" "true:2024-02-29:true:550e8400-e29b-41d4-a716-446655440000"
# Restart after completed snapshot without needing a first change event.
stop
start
assert_sql "SELECT count(*) FROM dst.items" "1"
sql "INSERT INTO src.items(id,name,amount) VALUES(2,repeat('toast-value-',2000),22.10); UPDATE src.items SET amount=999.99 WHERE id=1;" >/dev/null
assert_sql "SELECT amount FROM dst.items WHERE id=1" "999.99"
assert_sql "SELECT length(name) FROM dst.items WHERE id=2" "24000"
sql "UPDATE src.items SET amount=23.10 WHERE id=2; UPDATE src.items SET id=3 WHERE id=2; DELETE FROM src.items WHERE id=1;" >/dev/null
assert_sql "SELECT string_agg(id::text,',' ORDER BY id) FROM dst.items" "3"
assert_sql "SELECT length(name)||':'||amount FROM dst.items WHERE id=3" "24000:23.10"
stop
sql "UPDATE src.items SET name='after-restart' WHERE id=3; INSERT INTO src.items(id,name,amount) VALUES(4,'offline',4.10);" >/dev/null
start
assert_sql "SELECT name FROM dst.items WHERE id=3" "after-restart"
assert_sql "SELECT amount FROM dst.items WHERE id=4" "4.10"
sql "ALTER TABLE dst.items RENAME TO items_hold; INSERT INTO src.items(id,name,amount) VALUES(5,'retry',5.25);" >/dev/null
sleep 2
sql "ALTER TABLE dst.items_hold RENAME TO items;" >/dev/null
assert_sql "SELECT name FROM dst.items WHERE id=5" "retry"
# A hard process kill must replay from an intact atomic checkpoint.
kill -KILL "$pid"; wait "$pid" 2>/dev/null || [[ $? == 137 ]]; pid=""
sql "INSERT INTO src.items(id,name,amount) VALUES(6,'after-kill',6.25);" >/dev/null
start
assert_sql "SELECT name FROM dst.items WHERE id=6" "after-kill"
assert_sql "SELECT count(*) FROM dst.items" "4"
# Shape drift blocks a batch, then the same retained batch succeeds after repair.
sql "ALTER TABLE dst.items ADD COLUMN unexpected TEXT; INSERT INTO src.items(id,name,amount) VALUES(7,'schema-retry',7.25);" >/dev/null
sleep 2
[[ "$(sql 'SELECT count(*) FROM dst.items WHERE id=7')" == 0 ]] || { echo 'Target schema drift was ignored'; exit 1; }
sql "ALTER TABLE dst.items DROP COLUMN unexpected;" >/dev/null
assert_sql "SELECT name FROM dst.items WHERE id=7" "schema-retry"
stop
# Advancing a retained slot externally must not make the runner silently skip it.
sql "INSERT INTO src.items(id,name,amount) VALUES(8,'externally-consumed',8.25);" >/dev/null
sql "SELECT pg_replication_slot_advance('cdc_test_slot',pg_current_wal_lsn());" >/dev/null
expect_blocked
[[ "$(sql 'SELECT count(*) FROM dst.items WHERE id=8')" == 0 ]] || { echo 'Externally advanced slot was silently accepted'; exit 1; }
# A missing or recreated slot cannot silently become a new starting point.
sql "SELECT pg_drop_replication_slot('cdc_test_slot');" >/dev/null
expect_blocked
[[ "$(sql "SELECT count(*) FROM pg_replication_slots WHERE slot_name='cdc_test_slot'")" == 0 ]] || { echo 'Missing slot was recreated'; exit 1; }
sql "INSERT INTO src.items(id,name,amount) VALUES(9,'lost-slot-gap',9.25);" >/dev/null
sql "SELECT pg_create_logical_replication_slot('cdc_test_slot','pgoutput');" >/dev/null
expect_blocked
[[ "$(sql 'SELECT count(*) FROM dst.items WHERE id IN (8,9)')" == 0 ]] || { echo 'Unexpected skip or replacement snapshot'; exit 1; }
# A separate disposable task proves Debezium does not filter TRUNCATE away.
sql "CREATE TABLE src.truncate_probe(id BIGINT PRIMARY KEY,name TEXT);
CREATE TABLE dst.truncate_probe(LIKE src.truncate_probe INCLUDING ALL);
ALTER TABLE src.truncate_probe REPLICA IDENTITY FULL;
CREATE PUBLICATION cdc_truncate_pub FOR TABLE src.truncate_probe;" >/dev/null
sql "SELECT pg_create_logical_replication_slot('cdc_truncate_slot','pgoutput');" >/dev/null
python3 - <<'PY'
import os,json,pathlib
work=pathlib.Path(os.environ['CDC_TEST_WORK'])
task=json.loads((work/'task.json').read_text());task['id']='postgresql-truncate'
task['source']['options'].update(table='truncate_probe',slot='cdc_truncate_slot',publication='cdc_truncate_pub')
task['sink']['options']['table']='truncate_probe'
p=work/'truncate.json';p.write_text(json.dumps(task));p.chmod(0o600)
PY
export CDC_DATA_DIR="$work/truncate-state" CDC_TASK_FILE="$work/truncate.json"
start
# An empty initial snapshot must persist completion before any live data exists.
checkpoint_ready=false
for i in $(seq 1 45); do
  kill -0 "$pid" 2>/dev/null || { echo 'Empty snapshot runner exited'; exit 1; }
  if python3 - "$CDC_DATA_DIR/pg-offsets.json" <<'PY'
import sys,json,base64,pathlib
p=pathlib.Path(sys.argv[1])
if not p.is_file(): sys.exit(1)
state=json.loads(p.read_text());offsets=[json.loads(base64.b64decode(v)) for v in state['entries'].values()]
ready=bool(offsets) and all(o.get('snapshot') in (None,False,'false') or o.get('snapshot_completed') is True or o.get('last_snapshot_record') is True for o in offsets)
sys.exit(0 if ready else 1)
PY
  then checkpoint_ready=true; break; fi
  sleep 1
done
[[ "$checkpoint_ready" == true ]] || { echo 'Empty snapshot completion was not durable'; exit 1; }
stop
start
sql "INSERT INTO src.truncate_probe VALUES(1,'before-truncate');" >/dev/null
assert_sql 'SELECT count(*) FROM dst.truncate_probe' '1'
sql "TRUNCATE src.truncate_probe; INSERT INTO src.truncate_probe VALUES(2,'after-truncate');" >/dev/null
for i in $(seq 1 45); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
if kill -0 "$pid" 2>/dev/null; then echo 'TRUNCATE did not block capture'; exit 1; fi
status=0; wait "$pid" || status=$?; pid=""
[[ "$status" == 2 ]] || { echo "TRUNCATE returned unexpected exit $status"; exit 1; }
[[ "$(sql 'SELECT string_agg(id::text,chr(44) ORDER BY id) FROM dst.truncate_probe')" == 1 ]] || { echo 'Capture advanced beyond TRUNCATE'; exit 1; }
expect_blocked
echo 'PostgreSQL snapshot including empty-table restart, typed values, TOAST, DML, key change, clean/crash restart, retained retry, schema drift, slot advancement/loss and TRUNCATE blocking passed.'
