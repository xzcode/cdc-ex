#!/usr/bin/env bash
# Reproducible, bounded acceptance workload. Operates only on a disposable container.
set -euo pipefail
umask 077
jar=$(realpath "${1:-cdc-server/target/cdc-server-0.1.0-rc.1.jar}")
report=${CDC_ACCEPTANCE_REPORT:-acceptance-results.json}
work=$(mktemp -d)
name="cdcex-acceptance-$RANDOM-$$"
password=$(openssl rand -hex 18)
pid=''
cleanup() {
  rc=$?
  if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ $rc != 0 ]]; then tail -n 100 "$work/worker.log" 2>/dev/null || true; fi
  docker rm -fv "$name" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$rc"
}
trap cleanup EXIT
docker run -d --name "$name" --memory=512m --cpus=1 -p 127.0.0.1::3306 \
  -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_ROOT_HOST=% mysql:8.4.13 \
  --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
  --innodb-buffer-pool-size=64M --innodb-log-buffer-size=8M --max-connections=32 --performance-schema=OFF >/dev/null
port=$(docker port "$name" 3306/tcp | head -n1 | awk -F: '{print $NF}')
sql() { docker exec -e MYSQL_PWD="$password" "$name" mysql -uroot -N -B -e "$1"; }
for _ in $(seq 1 90); do if sql 'SELECT 1' >/dev/null 2>&1; then break; fi; sleep 1; done
sql 'SELECT 1' >/dev/null
version=$(sql 'SELECT VERSION()')
sql 'CREATE DATABASE src; CREATE DATABASE dst; CREATE TABLE src.items(id BIGINT PRIMARY KEY,name VARCHAR(80),amount DECIMAL(18,2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci; CREATE TABLE dst.items LIKE src.items;'
python3 - <<'PY' >"$work/initial.sql"
for start in range(1,1001,250):
    rows=','.join("(%d,'record-%d',%d.%02d)"%(i,i,i//100,i%100) for i in range(start,min(start+250,1001)))
    print('INSERT INTO src.items VALUES '+rows+';')
PY
docker exec -i -e MYSQL_PWD="$password" "$name" mysql -uroot <"$work/initial.sql"
cat >"$work/task.json" <<JSON
{"id":"acceptance","source":{"type":"MYSQL","options":{"host":"127.0.0.1","port":"$port","user":"root","password":"$password","database":"src","table":"items","sslMode":"REQUIRED","replicationServerId":"54019"}},"sink":{"type":"MYSQL","options":{"host":"127.0.0.1","port":"$port","user":"root","password":"$password","database":"dst","table":"items","sslMode":"REQUIRED"}},"snapshotMode":"initial","maxFailures":4}
JSON
start() { CDC_RUN=true CDC_TASK_FILE="$work/task.json" CDC_DATA_DIR="$work/state" java -jar "$jar" >>"$work/worker.log" 2>&1 & pid=$!; }
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=''; }
wait_for() {
  for _ in $(seq 1 120); do
    if [[ $(sql "$1" 2>/dev/null) == "$2" ]]; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then echo 'Acceptance worker exited unexpectedly' >&2; return 1; fi
    sleep 0.25
  done
  echo 'Acceptance assertion timed out' >&2; return 1
}
clock() { python3 -c 'import time;print(time.monotonic_ns())'; }
begin=$(clock)
start
wait_for 'SELECT COUNT(*) FROM dst.items' 1000
snapshot_end=$(clock)
python3 - <<'PY' >"$work/dml.sql"
print('START TRANSACTION;')
for start in range(1001,5001,250):
    rows=','.join("(%d,'record-%d',%d.%02d)"%(i,i,i//100,i%100) for i in range(start,min(start+250,5001)))
    print('INSERT INTO src.items VALUES '+rows+';')
print("UPDATE src.items SET name=CONCAT(name,'-updated'),amount=amount+1.25 WHERE id<=1000;")
print('DELETE FROM src.items WHERE MOD(id,10)=0;COMMIT;')
PY
dml_begin=$(clock)
docker exec -i -e MYSQL_PWD="$password" "$name" mysql -uroot <"$work/dml.sql"
wait_for "SELECT COUNT(*)=4500 AND SUM(name LIKE '%-updated')=900 FROM dst.items" 1
dml_end=$(clock)
hash_table() { sql "SELECT id,name,amount FROM $1.items ORDER BY id" | sha256sum | awk '{print $1}'; }
[[ $(hash_table src) == "$(hash_table dst)" ]]
# An ambiguous crash can happen after a destination commit but before its local watermark.
kill -KILL "$pid"; wait "$pid" 2>/dev/null || true; pid=''
sql "INSERT INTO src.items VALUES(6001,'after-crash',60.01)"
start
wait_for 'SELECT COUNT(*) FROM dst.items WHERE id=6001' 1
# Incompatible destination DDL retains the pending event; repair then retry the same generation.
sql 'ALTER TABLE dst.items ADD COLUMN unexpected INT NULL'
sql "INSERT INTO src.items VALUES(6002,'after-repair',60.02)"
for _ in $(seq 1 80); do if ! kill -0 "$pid" 2>/dev/null; then break; fi; sleep 0.25; done
if kill -0 "$pid" 2>/dev/null; then echo 'Schema drift did not block' >&2; exit 1; fi
if wait "$pid"; then echo 'Schema drift unexpectedly succeeded' >&2; exit 1; fi
pid=''
python3 - "$work/state/journal.db" <<'PY'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
    meta=dict(db.execute('SELECT k,v FROM metadata'))
    assert int(meta['event_count'])-int(meta['consumed_count'])>0,'Failed event was not retained'
PY
sql 'ALTER TABLE dst.items DROP COLUMN unexpected'
start
wait_for 'SELECT COUNT(*) FROM dst.items WHERE id=6002' 1
[[ $(hash_table src) == "$(hash_table dst)" ]]
stop
# Expired native log positions must block; never silently resnapshot or jump to latest.
sql 'FLUSH BINARY LOGS'
latest=$(sql 'SHOW BINARY LOGS' | tail -n1 | cut -f1)
[[ $latest =~ ^mysql-bin\.[0-9]+$ ]]
sql "PURGE BINARY LOGS TO '$latest'; INSERT INTO src.items VALUES(6003,'expired-log',60.03)"
start
for _ in $(seq 1 120); do if ! kill -0 "$pid" 2>/dev/null; then break; fi; sleep 0.25; done
if kill -0 "$pid" 2>/dev/null; then echo 'Purged binlog did not block' >&2; exit 1; fi
if wait "$pid"; then echo 'Purged binlog unexpectedly succeeded' >&2; exit 1; fi
pid=''
[[ $(sql 'SELECT COUNT(*) FROM dst.items WHERE id=6003') == 0 ]]
python3 - "$report" "$version" "$begin" "$snapshot_end" "$dml_begin" "$dml_end" <<'PY'
import datetime,json,pathlib,sys
path,version,begin,snapshot_end,dml_begin,dml_end=sys.argv[1:]
snapshot=(int(snapshot_end)-int(begin))/1e9
dml=(int(dml_end)-int(dml_begin))/1e9
result={'testedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'mysqlVersion':version,'snapshotRows':1000,'snapshotSeconds':round(snapshot,3),'dmlEvents':5500,'dmlSeconds':round(dml,3),'dmlEventsPerSecond':round(5500/dml,1),'validatedRowsBeforeExpiryTest':4502,'keyedPayloadSha256Equal':True,'hardCrashRecovery':'passed','schemaDriftAndRepair':'passed','expiredBinlogBlocks':'passed','scope':'Disposable single-core 512 MiB MySQL; shared host; startup and polling overhead included. This is an acceptance workload, not a general throughput guarantee.'}
pathlib.Path(path).parent.mkdir(parents=True,exist_ok=True)
pathlib.Path(path).write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
PY
