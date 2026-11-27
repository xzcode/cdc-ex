#!/usr/bin/env bash
# Real management repositories and active-task-count regression; disposable fixtures only.
set -euo pipefail
umask 077
jar=$(realpath "$1")
work=$(mktemp -d)
mysql="cdcex-control-sql-$$-$RANDOM"
mongo="cdcex-control-mongo-$$-$RANDOM"
secret=$(openssl rand -hex 24)
export CONTROL_TEST_SECRET="$secret" CONTROL_TEST_WORK="$work" CONTROL_TEST_JAR="$jar"
export CDC_TEST_CONTROL_TYPE="${CDC_TEST_CONTROL_TYPE:-SQLITE}"
cleanup(){
  rc=$?
  if [[ $rc != 0 ]]; then
    python3 - "$work" "$secret" <<'PY'
import pathlib,sys
for p in pathlib.Path(sys.argv[1]).glob('*.log'):
    print(p.name, '\n'.join(p.read_text(errors='replace').splitlines()[-30:]).replace(sys.argv[2],'[redacted]'))
PY
  fi
  docker rm -fv "$mysql" "$mongo" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$rc"
}
trap cleanup EXIT
docker run -d --name "$mysql" --memory=512m --cpus=1 -p 127.0.0.1::3306 -e MYSQL_ROOT_PASSWORD="$secret" -e MYSQL_ROOT_HOST=% mysql:8.4.13 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --innodb-buffer-pool-size=64M --performance-schema=OFF --max-connections=100 >/dev/null
sql(){ docker exec -e MYSQL_PWD="$secret" "$mysql" mysql -h127.0.0.1 -uroot -NB -e "$1"; }
for i in $(seq 1 90); do if sql 'SELECT 1' >/dev/null 2>&1; then break; fi; sleep 1; done
sql 'CREATE DATABASE src; CREATE DATABASE dst; CREATE DATABASE cdc_control; CREATE DATABASE cdc_empty; CREATE TABLE src.items(id BIGINT PRIMARY KEY,name VARCHAR(40)); INSERT INTO src.items VALUES(1,"initial");'
count=1
if [[ "$CDC_TEST_CONTROL_TYPE" == MYSQL ]]; then count=5; fi
for i in $(seq 1 "$count"); do sql "CREATE TABLE dst.items_$i LIKE src.items"; done
export CONTROL_TEST_COUNT="$count" CONTROL_TEST_SQL_CONTAINER="$mysql"
export CONTROL_TEST_SQL_PORT="$(docker port "$mysql" 3306/tcp | head -1 | awk -F: '{print $NF}')"
if [[ "$CDC_TEST_CONTROL_TYPE" == MONGODB ]]; then
  docker run -d --name "$mongo" --memory=512m --cpus=1 -p 127.0.0.1::27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD="$secret" mongo:8.0.15 --wiredTigerCacheSizeGB=0.25 >/dev/null
  for i in $(seq 1 90); do
    if docker exec "$mongo" mongosh --quiet --host 127.0.0.1 -u root -p "$secret" --authenticationDatabase admin --eval 'db.adminCommand({ping:1}).ok' 2>/dev/null | grep -qx 1; then break; fi
    sleep 1
  done
  export CONTROL_TEST_MONGO_CONTAINER="$mongo"
  export CONTROL_TEST_MONGO_PORT="$(docker port "$mongo" 27017/tcp | head -1 | awk -F: '{print $NF}')"
fi
python3 scripts/control-store-smoke.py
