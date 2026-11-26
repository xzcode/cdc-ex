#!/usr/bin/env bash
set -euo pipefail
jar="$(realpath "${1:-cdc-server/target/cdc-server-0.1.0-SNAPSHOT.jar}")"
work="$(mktemp -d)"
container="cdc-ex-test-$$-$RANDOM"
password="$(openssl rand -hex 18)"
pid=""
cleanup() {
  rc=$?
  if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ "$rc" != 0 ]]; then tail -n 100 "$work/runner.log" 2>/dev/null || true; docker logs --tail 30 "$container" 2>/dev/null || true; fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$work"
  return "$rc"
}
trap cleanup EXIT
docker run -d --name "$container" -p 127.0.0.1::3306 -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_ROOT_HOST=% mysql:8.4 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL >/dev/null
sql() { docker exec -e MYSQL_PWD="$password" "$container" mysql -uroot -N -B -e "$1"; }
for i in $(seq 1 90); do if docker exec -e MYSQL_PWD="$password" "$container" mysql -h127.0.0.1 -uroot -N -B -e "SELECT 1" >/dev/null 2>&1; then break; fi; sleep 2; done
sql "CREATE DATABASE src; CREATE DATABASE dst; CREATE TABLE src.items(id BIGINT PRIMARY KEY, name VARCHAR(40), amount DECIMAL(18,2)) ENGINE=InnoDB; CREATE TABLE dst.items LIKE src.items; INSERT INTO src.items VALUES(1,'initial',123.45);"
port="$(docker port "$container" 3306/tcp | head -1 | sed 's/.*://')"
export CDC_RUN=true CDC_DATA_DIR="$work/state" CDC_REPLICATION_SERVER_ID=47123 CDC_SNAPSHOT_MODE=initial
export CDC_SOURCE_HOST=127.0.0.1 CDC_SOURCE_PORT="$port" CDC_SOURCE_USER=root CDC_SOURCE_PASSWORD="$password" CDC_SOURCE_DATABASE=src CDC_SOURCE_TABLE=items CDC_SOURCE_SSL_MODE=REQUIRED
export CDC_TARGET_HOST=127.0.0.1 CDC_TARGET_PORT="$port" CDC_TARGET_USER=root CDC_TARGET_PASSWORD="$password" CDC_TARGET_DATABASE=dst CDC_TARGET_TABLE=items CDC_TARGET_SSL_MODE=REQUIRED
CDC_PREFLIGHT_ONLY=true java -jar "$jar"
start() { java -jar "$jar" >>"$work/runner.log" 2>&1 & pid=$!; }
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=""; }
assert_sql() {
  local query="$1" expected="$2" actual
  for i in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "Runner exited"; return 1; }
    actual="$(sql "$query")"
    if [[ "$actual" == "$expected" ]]; then return 0; fi
    sleep 1
  done
  echo "Expected $expected, got $actual"; return 1
}
start
assert_sql "SELECT CONCAT(name,':',amount) FROM dst.items WHERE id=1" "initial:123.45"
sql "INSERT INTO src.items VALUES(2,'added',22.10); UPDATE src.items SET amount=999.99 WHERE id=1;"
assert_sql "SELECT amount FROM dst.items WHERE id=1" "999.99"
assert_sql "SELECT name FROM dst.items WHERE id=2" "added"
sql "UPDATE src.items SET id=3 WHERE id=2; DELETE FROM src.items WHERE id=1;"
assert_sql "SELECT GROUP_CONCAT(id ORDER BY id) FROM dst.items" "3"
stop
sql "UPDATE src.items SET name='after-restart' WHERE id=3; INSERT INTO src.items VALUES(4,'offline',4.10);"
start
assert_sql "SELECT name FROM dst.items WHERE id=3" "after-restart"
assert_sql "SELECT amount FROM dst.items WHERE id=4" "4.10"
sql "RENAME TABLE dst.items TO dst.items_hold; INSERT INTO src.items VALUES(5,'retry',5.25);"
sleep 2
sql "RENAME TABLE dst.items_hold TO dst.items;"
assert_sql "SELECT name FROM dst.items WHERE id=5" "retry"
stop
echo "MySQL snapshot, DML, primary-key change, restart and retained sink retry passed."
