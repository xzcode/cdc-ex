#!/usr/bin/env bash
# Isolated MySQL / MariaDB acceptance, including cross-dialect directions.
set -euo pipefail
jar="$(realpath "${1:-cdc-server/target/cdc-server-0.1.0-rc.1.jar}")"
work="$(mktemp -d)"
src="cdc-ex-src-$$-$RANDOM"
dst="cdc-ex-dst-$$-$RANDOM"
st="${CDC_TEST_SOURCE_TYPE:-MYSQL}"
tt="${CDC_TEST_TARGET_TYPE:-MYSQL}"
password="$(openssl rand -hex 18)"
pid=""
cleanup() {
  rc=$?
  if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ "$rc" != 0 ]]; then tail -n 100 "$work/runner.log" 2>/dev/null || true; docker logs --tail 30 "$src" 2>/dev/null || true; docker logs --tail 30 "$dst" 2>/dev/null || true; fi
  docker rm -fv "$src" >/dev/null 2>&1 || true
  if [[ "$src" != "$dst" ]]; then docker rm -fv "$dst" >/dev/null 2>&1 || true; fi
  rm -rf "$work"
  return "$rc"
}
trap cleanup EXIT
start_db() {
  local name="$1" type="$2" sid="$3"
  case "$type" in
    MYSQL) docker run -d --name "$name" --memory=512m --cpus=1 -p 127.0.0.1::3306 -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_ROOT_HOST=% mysql:8.4.13 --innodb-buffer-pool-size=64M --innodb-log-buffer-size=8M --max-connections=32 --performance-schema=OFF --server-id="$sid" --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL >/dev/null ;;
    MARIADB) docker run -d --name "$name" --memory=512m --cpus=1 -p 127.0.0.1::3306 -e MARIADB_ROOT_PASSWORD="$password" -e MARIADB_ROOT_HOST=% mariadb:11.4.13 --innodb-buffer-pool-size=64M --innodb-log-buffer-size=8M --max-connections=32 --performance-schema=OFF --server-id="$sid" --log-bin=mariadb-bin --binlog-format=ROW --binlog-row-image=FULL --binlog-legacy-event-pos=ON --log-bin-compress=OFF >/dev/null ;;
    *) echo "Unsupported test dialect"; return 1 ;;
  esac
}
client() { if [[ "$1" == MARIADB ]]; then printf mariadb; else printf mysql; fi; }
sql() { local container="$1" type="$2"; shift 2; docker exec -e MYSQL_PWD="$password" "$container" "$(client "$type")" -uroot -N -B "$@"; }
ready() {
  for i in $(seq 1 90); do if sql "$1" "$2" -h127.0.0.1 -e "SELECT 1" >/dev/null 2>&1; then return 0; fi; sleep 2; done
  echo "Database not ready"; return 1
}
start_db "$src" "$st" 1
if [[ "$st" == "$tt" ]]; then dst="$src"; else start_db "$dst" "$tt" 2; fi
ready "$src" "$st"
if [[ "$dst" != "$src" ]]; then ready "$dst" "$tt"; fi
s_sql(){ sql "$src" "$st" -e "$1"; }
t_sql(){ sql "$dst" "$tt" -e "$1"; }
definition="(id BIGINT PRIMARY KEY, name VARCHAR(40), amount DECIMAL(18,2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
s_sql "CREATE DATABASE src; CREATE TABLE src.items $definition; INSERT INTO src.items VALUES(1,'initial',123.45);"
t_sql "CREATE DATABASE dst; CREATE TABLE dst.items $definition;"
echo "Source $(s_sql 'SELECT VERSION()') => target $(t_sql 'SELECT VERSION()')"
source_port="$(docker port "$src" 3306/tcp | head -1 | sed 's/.*://')"
target_port="$(docker port "$dst" 3306/tcp | head -1 | sed 's/.*://')"
export CDC_RUN=true CDC_DATA_DIR="$work/state" CDC_REPLICATION_SERVER_ID=47123 CDC_SNAPSHOT_MODE=initial
export CDC_SOURCE_TYPE="$st" CDC_SOURCE_HOST=127.0.0.1 CDC_SOURCE_PORT="$source_port" CDC_SOURCE_USER=root CDC_SOURCE_PASSWORD="$password" CDC_SOURCE_DATABASE=src CDC_SOURCE_TABLE=items CDC_SOURCE_SSL_MODE=REQUIRED
export CDC_TARGET_TYPE="$tt" CDC_TARGET_HOST=127.0.0.1 CDC_TARGET_PORT="$target_port" CDC_TARGET_USER=root CDC_TARGET_PASSWORD="$password" CDC_TARGET_DATABASE=dst CDC_TARGET_TABLE=items CDC_TARGET_SSL_MODE=REQUIRED
CDC_PREFLIGHT_ONLY=true java -jar "$jar"
start() {
  local previous=0
  if [[ -f "$work/runner.log" ]]; then previous=$(grep -c 'CDC EX task started:' "$work/runner.log" || true); fi
  java -jar "$jar" >>"$work/runner.log" 2>&1 & pid=$!
  for _ in $(seq 1 120); do
    if (( $(grep -c 'CDC EX task started:' "$work/runner.log" || true) > previous )); then return; fi
    kill -0 "$pid" 2>/dev/null || { echo 'Runner failed during preflight' >&2; return 1; }
    sleep 0.25
  done
  echo 'Runner did not finish preflight' >&2; return 1
}
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=""; }
assert_sql() {
  local query="$1" expected="$2" actual
  for i in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "Runner exited"; return 1; }
    actual="$(t_sql "$query")"
    if [[ "$actual" == "$expected" ]]; then return 0; fi
    sleep 1
  done
  echo "Expected $expected, got $actual"; return 1
}
start
assert_sql "SELECT CONCAT(name,':',amount) FROM dst.items WHERE id=1" "initial:123.45"
# Snapshot-only restart must not require a subsequent DML event as proof of completion.
stop
start
assert_sql "SELECT CONCAT(name,':',amount) FROM dst.items WHERE id=1" "initial:123.45"
s_sql "INSERT INTO src.items VALUES(2,'added',22.10); UPDATE src.items SET amount=999.99 WHERE id=1;"
assert_sql "SELECT amount FROM dst.items WHERE id=1" "999.99"
assert_sql "SELECT name FROM dst.items WHERE id=2" "added"
s_sql "UPDATE src.items SET id=3 WHERE id=2; DELETE FROM src.items WHERE id=1;"
assert_sql "SELECT GROUP_CONCAT(id ORDER BY id) FROM dst.items" "3"
stop
s_sql "UPDATE src.items SET name='after-restart' WHERE id=3; INSERT INTO src.items VALUES(4,'offline',4.10);"
start
assert_sql "SELECT name FROM dst.items WHERE id=3" "after-restart"
assert_sql "SELECT amount FROM dst.items WHERE id=4" "4.10"
t_sql "RENAME TABLE dst.items TO dst.items_hold;"
s_sql "INSERT INTO src.items VALUES(5,'retry',5.25);"
sleep 2
t_sql "RENAME TABLE dst.items_hold TO dst.items;"
assert_sql "SELECT name FROM dst.items WHERE id=5" "retry"
stop
# Empty snapshots still need a durable native completion offset, before any DML.
s_sql "CREATE TABLE src.empty_items $definition;"
t_sql "CREATE TABLE dst.empty_items $definition;"
export CDC_SOURCE_TABLE=empty_items CDC_TARGET_TABLE=empty_items CDC_DATA_DIR="$work/empty-state"
start
checkpoint_ready=false
for _ in $(seq 1 120); do
  kill -0 "$pid" 2>/dev/null || { echo 'Empty snapshot exited unexpectedly' >&2; exit 1; }
  if python3 - "$CDC_DATA_DIR/offsets.dat" <<'PY'
import sys,json,base64,pathlib
path=pathlib.Path(sys.argv[1])
if not path.is_file(): sys.exit(1)
state=json.loads(path.read_text())
offsets=[]
for encoded in state['entries'].values():
    if encoded is None: continue
    offset=json.loads(base64.b64decode(encoded))
    if 'schema' in offset and 'payload' in offset: offset=offset['payload']
    offsets.append(offset)
ready=bool(offsets) and all(o.get('snapshot') in (None,False,'false') or o.get('snapshot_completed') is True or o.get('last_snapshot_record') is True for o in offsets)
sys.exit(0 if ready else 1)
PY
  then checkpoint_ready=true; break; fi
  sleep 0.25
done
[[ "$checkpoint_ready" == true ]] || { echo 'Empty snapshot completion was not durable' >&2; exit 1; }
stop
start
s_sql "INSERT INTO src.empty_items VALUES(1,'empty-resumed',1.25)"
assert_sql "SELECT name FROM dst.empty_items WHERE id=1" 'empty-resumed'
stop
echo "$st -> $tt snapshot, empty snapshot, DML, primary-key change, restart and retained sink retry passed."
