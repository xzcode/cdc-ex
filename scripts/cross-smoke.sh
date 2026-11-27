#!/usr/bin/env bash
# Isolated heterogeneous fixtures. Every task is one-way and has its own source/target scope.
set -euo pipefail
jar=$(realpath "${1:-cdc-server/target/cdc-server-$(cat VERSION).jar}")
work=$(mktemp -d)
mysql_name="cdcex-cross-sql-$RANDOM-$$"
mongo_name="cdcex-cross-mongo-$RANDOM-$$"
pg_name="cdcex-cross-pg-$RANDOM-$$"
ch_name="cdcex-cross-ch-$RANDOM-$$"
password=$(openssl rand -hex 18)
pid=''
suite=${CDC_CROSS_SUITE:-all}
cleanup() {
  result=$?
  if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ $result -ne 0 ]]; then
    tail -n 120 "$work/worker.log" 2>/dev/null || true
    # These are bounded rows from this script's disposable fixtures; never print task files or credentials.
    if [[ "$suite" == all || "$suite" == mongodb ]]; then
      echo 'Mongo cross fixture diagnostics: SQL source rows and connection character sets' >&2
      sql 'SELECT @@character_set_client,@@character_set_connection,@@character_set_results;
SELECT id,name,HEX(name),amount FROM srcsql.items ORDER BY id LIMIT 10;
SELECT id,name,amount FROM dstsql.items ORDER BY id LIMIT 10;' >&2 || true
      if declare -F mongo >/dev/null; then
        mongo 'for(const name of ["mongodst","mongosrc"]){const d=db.getSiblingDB(name);print(name+".items="+EJSON.stringify(d.items.find().limit(10).toArray(),{relaxed:false}));print(name+".types="+EJSON.stringify(d.items.aggregate([{$limit:10},{$project:{_id:1,idType:{$type:"$_id"},amountType:{$type:"$amount"},name:1,amount:1}}]).toArray(),{relaxed:false}));}' >&2 || true
      fi
    fi
  fi
  for name in "$mysql_name" "$mongo_name" "$pg_name" "$ch_name"; do docker rm -fv "$name" >/dev/null 2>&1 || true; done
  rm -rf "$work"
  exit "$result"
}
trap cleanup EXIT
case "$suite" in all|mongodb|postgresql|clickhouse) ;; *) echo 'Unknown cross-suite selection' >&2; exit 1;; esac
docker run -d --name "$mysql_name" --memory=512m --cpus=1 -p 127.0.0.1::3306 \
  -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_ROOT_HOST=% mysql:8.4.13 \
  --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
  --innodb-buffer-pool-size=64M --performance-schema=OFF --max-connections=30 >/dev/null
sql() { docker exec -e MYSQL_PWD="$password" "$mysql_name" mysql --default-character-set=utf8mb4 -h127.0.0.1 -uroot -NB -e "$1"; }
ready=false
for i in $(seq 1 90); do if sql 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi; sleep 1; done
[[ "$ready" == true ]]
mysql_port=$(docker port "$mysql_name" 3306/tcp | head -n1 | awk -F: '{print $NF}')
sql 'SELECT VERSION(); CREATE DATABASE srcsql; CREATE DATABASE dstsql;'
definition='(id BIGINT PRIMARY KEY,name VARCHAR(40),amount DECIMAL(18,2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci'
start() { CDC_RUN=true CDC_TASK_FILE="$work/$1.json" CDC_DATA_DIR="$work/$1-state" java -jar "$jar" >>"$work/worker.log" 2>&1 & pid=$!; }
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=''; }
assert_sql() {
  local query=$1 expected=$2
  for i in $(seq 1 90); do
    if [[ $(sql "$query" 2>/dev/null) == "$expected" ]]; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then echo 'Cross-task worker exited unexpectedly' >&2; return 1; fi
    sleep 1
  done
  echo 'Cross-task SQL assertion timed out' >&2; return 1
}

if [[ "$suite" == all || "$suite" == mongodb ]]; then
  docker run -d --name "$mongo_name" --memory=768m --cpus=1 -p 127.0.0.1::27017 \
    mongo:8.0.15 --replSet rs0 --bind_ip_all --wiredTigerCacheSizeGB=0.25 >/dev/null
  mongo() { docker exec "$mongo_name" mongosh --quiet --eval "$1"; }
  for i in $(seq 1 60); do if mongo 'db.adminCommand({ping:1}).ok' >/dev/null 2>&1; then break; fi; sleep 1; done
  mongo 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})' >/dev/null
  for i in $(seq 1 60); do if [[ $(mongo 'db.hello().isWritablePrimary' 2>/dev/null) == true ]]; then break; fi; sleep 1; done
  mongo_port=$(docker port "$mongo_name" 27017/tcp | head -n1 | awk -F: '{print $NF}')
  mongo 'db=db.getSiblingDB("mongodst");db.createCollection("items");db=db.getSiblingDB("mongosrc");db.createCollection("items",{changeStreamPreAndPostImages:{enabled:true}});db.createCollection("objects",{changeStreamPreAndPostImages:{enabled:true}});db.items.insertOne({_id:NumberLong("9007199254740993"),name:"mongo-initial",amount:NumberDecimal("1234567890123456.78")});db.objects.insertOne({_id:ObjectId("507f1f77bcf86cd799439011"),name:"object-id",amount:NumberDecimal("1.23")});print(db.version());' 
  sql "CREATE TABLE srcsql.items $definition; CREATE TABLE dstsql.items $definition;
CREATE TABLE dstsql.objects(id VARCHAR(24) COLLATE utf8mb4_bin PRIMARY KEY,name VARCHAR(40),amount DECIMAL(18,2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
INSERT INTO srcsql.items VALUES(1,'初始数据',123.45),(9007199254740993,'large-integer',999.99);"
  # Validate the bytes inserted by the fixture before testing Unicode replication.
  [[ $(sql 'SELECT HEX(name) FROM srcsql.items WHERE id=1') == E5889DE5A78BE695B0E68DAE ]]
  python3 - "$work" "$mysql_port" "$mongo_port" "$password" <<'PY'
import json,pathlib,sys
folder,sqlport,mongoport,password=sys.argv[1:]
mysql=dict(host='127.0.0.1',port=sqlport,user='root',password=password,sslMode='REQUIRED',table='items')
mongo=dict(host='127.0.0.1',port=mongoport,tls='false',allowInsecure='true',directConnection='true',replicaSet='rs0',table='items')
tasks=[
 ('sql-mongo',dict(type='MYSQL',options=dict(mysql,database='srcsql',replicationServerId='47301')),dict(type='MONGODB',options=dict(mongo,database='mongodst',documentMode='replace')),dict(rename={'id':'_id'},conversions={'_id':'BSON_INT64','amount':'BSON_DECIMAL'},exclude=[])),
 ('mongo-sql',dict(type='MONGODB',options=dict(mongo,database='mongosrc')),dict(type='MYSQL',options=dict(mysql,database='dstsql')),dict(rename={'_id':'id'},conversions={'id':'LONG','amount':'DECIMAL'},exclude=[])),
 ('object-sql',dict(type='MONGODB',options=dict(mongo,database='mongosrc',table='objects')),dict(type='MYSQL',options=dict(mysql,database='dstsql',table='objects')),dict(rename={'_id':'id'},conversions={'id':'STRING','amount':'DECIMAL'},exclude=[]))]
for name,source,sink,mapping in tasks:
 task=dict(id=name,source=source,sink=sink,snapshotMode='initial',mapping=mapping,journalBytes=268435456,journalEvents=1000000,batchSize=128,maxFailures=30)
 path=pathlib.Path(folder,name+'.json');path.write_text(json.dumps(task));path.chmod(0o600)
PY
  assert_mongo() {
    local query=$1 actual
    for i in $(seq 1 90); do
      # A malformed fixture query is an immediate failure, not replication lag.
      if ! actual=$(mongo "$query"); then
        echo 'Cross-task MongoDB assertion command failed' >&2
        return 1
      fi
      if [[ "$actual" == true ]]; then return; fi
      if ! kill -0 "$pid" 2>/dev/null; then echo 'Cross-task worker exited unexpectedly' >&2; return 1; fi
      sleep 1
    done
    echo "Cross-task MongoDB assertion timed out: $query (last result: $actual)" >&2; return 1
  }
  start sql-mongo
  assert_mongo 'const d=db.getSiblingDB("mongodst");print(d.items.countDocuments({})===2&&d.items.findOne({_id:NumberLong("1")}).name==="初始数据"&&d.items.findOne({_id:NumberLong("9007199254740993")}).amount.toString()==="999.99"&&d.items.countDocuments({_id:{$type:"long"}})===2)'
  sql "UPDATE srcsql.items SET amount=42.10 WHERE id=1; UPDATE srcsql.items SET id=3 WHERE id=9007199254740993; INSERT INTO srcsql.items VALUES(4,'delete-me',4.10); DELETE FROM srcsql.items WHERE id=4;"
  assert_mongo 'const d=db.getSiblingDB("mongodst");print(d.items.countDocuments({})===2&&d.items.findOne({_id:NumberLong("1")}).amount.toString()==="42.10"&&d.items.findOne({_id:NumberLong("3")})!==null&&d.items.findOne({_id:NumberLong("9007199254740993")})===null)'
  stop
  sql "INSERT INTO srcsql.items VALUES(5,'offline',5.55)"
  start sql-mongo
  assert_mongo 'print(db.getSiblingDB("mongodst").items.findOne({_id:NumberLong("5")})?.name==="offline")'
  stop
  echo 'MySQL -> MongoDB explicit key/Decimal128 mapping, Unicode, DML, primary-key change and restart passed.'
  start mongo-sql
  assert_sql "SELECT CONCAT(id,':',amount) FROM dstsql.items" '9007199254740993:1234567890123456.78'
  mongo 'db=db.getSiblingDB("mongosrc");db.items.updateOne({_id:NumberLong("9007199254740993")},{$set:{name:"updated",amount:NumberDecimal("999.99")}});db.items.insertOne({_id:NumberLong("2"),name:"second",amount:NumberDecimal("2.20")});' >/dev/null
  assert_sql "SELECT CONCAT(name,':',amount) FROM dstsql.items WHERE id=9007199254740993" 'updated:999.99'
  assert_sql 'SELECT amount FROM dstsql.items WHERE id=2' '2.20'
  mongo 'db.getSiblingDB("mongosrc").items.deleteOne({_id:NumberLong("2")})' >/dev/null
  assert_sql 'SELECT count(*) FROM dstsql.items' 1
  stop
  mongo 'db.getSiblingDB("mongosrc").items.insertOne({_id:NumberLong("3"),name:"offline",amount:NumberDecimal("3.30")})' >/dev/null
  start mongo-sql
  assert_sql "SELECT CONCAT(name,':',amount) FROM dstsql.items WHERE id=3" 'offline:3.30'
  stop
  start object-sql
  assert_sql "SELECT CONCAT(id,':',amount) FROM dstsql.objects" '507f1f77bcf86cd799439011:1.23'
  mongo 'db.getSiblingDB("mongosrc").objects.updateOne({_id:ObjectId("507f1f77bcf86cd799439011")},{$set:{amount:NumberDecimal("9.87")}})' >/dev/null
  assert_sql 'SELECT amount FROM dstsql.objects' '9.87'
  mongo 'db.getSiblingDB("mongosrc").objects.deleteOne({_id:ObjectId("507f1f77bcf86cd799439011")})' >/dev/null
  assert_sql 'SELECT count(*) FROM dstsql.objects' 0
  stop
  echo 'MongoDB -> MySQL exact Int64/Decimal128 and explicit ObjectId-to-text mapping, update/delete and restart passed.'
  docker rm -fv "$mongo_name" >/dev/null
fi

if [[ "$suite" == all || "$suite" == postgresql ]]; then
  docker run -d --name "$pg_name" --memory=384m --cpus=1 -p 127.0.0.1::5432 -e POSTGRES_PASSWORD="$password" \
    postgres:17.7 -c shared_buffers=32MB -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 >/dev/null
  pg() { docker exec -e PGPASSWORD="$password" "$pg_name" psql -h127.0.0.1 -U postgres -d postgres -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
  ready=false
  for i in $(seq 1 60); do if pg 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi; sleep 1; done
  [[ "$ready" == true ]]
  pg_port=$(docker port "$pg_name" 5432/tcp | head -n1 | awk -F: '{print $NF}')
  pg 'SHOW server_version; CREATE SCHEMA src; CREATE SCHEMA dst;
CREATE TABLE src.items(id BIGINT PRIMARY KEY,name VARCHAR(40),amount NUMERIC(18,2));
CREATE TABLE dst.items(LIKE src.items INCLUDING ALL);
ALTER TABLE src.items REPLICA IDENTITY FULL;
CREATE PUBLICATION cross_pub FOR TABLE src.items;' 
  pg "SELECT pg_create_logical_replication_slot('cross_slot','pgoutput');" >/dev/null
  pg "INSERT INTO src.items VALUES(1,'pg-initial',999.99);" >/dev/null
  sql "CREATE TABLE srcsql.pg_items $definition; CREATE TABLE dstsql.pg_items $definition;
INSERT INTO srcsql.pg_items VALUES(1,'mysql-initial',123.45);"
  python3 - "$work" "$mysql_port" "$pg_port" "$password" <<'PY'
import json,pathlib,sys
folder,sqlport,pgport,password=sys.argv[1:]
mysql=dict(host='127.0.0.1',port=sqlport,user='root',password=password,sslMode='REQUIRED',table='pg_items')
pg=dict(host='127.0.0.1',port=pgport,user='postgres',password=password,sslMode='disable',allowInsecure='true',database='postgres',table='items')
for name,source,sink in [
 ('sql-pg',dict(type='MYSQL',options=dict(mysql,database='srcsql',replicationServerId='47302')),dict(type='POSTGRESQL',options=dict(pg,schema='dst'))),
 ('pg-sql',dict(type='POSTGRESQL',options=dict(pg,schema='src',slot='cross_slot',publication='cross_pub')),dict(type='MYSQL',options=dict(mysql,database='dstsql')))]:
 task=dict(id=name,source=source,sink=sink,snapshotMode='initial',mapping=dict(rename={},conversions={},exclude=[]),journalBytes=268435456,journalEvents=1000000,batchSize=128,maxFailures=30)
 path=pathlib.Path(folder,name+'.json');path.write_text(json.dumps(task));path.chmod(0o600)
PY
  assert_pg() {
    local query=$1 expected=$2
    for i in $(seq 1 90); do
      if [[ $(pg "$query" 2>/dev/null) == "$expected" ]]; then return; fi
      if ! kill -0 "$pid" 2>/dev/null; then echo 'Cross-task worker exited unexpectedly' >&2; return 1; fi
      sleep 1
    done
    echo 'Cross-task PostgreSQL assertion timed out' >&2; return 1
  }
  start sql-pg
  assert_pg "SELECT name||':'||amount FROM dst.items WHERE id=1" 'mysql-initial:123.45'
  sql "UPDATE srcsql.pg_items SET amount=1.20 WHERE id=1; INSERT INTO srcsql.pg_items VALUES(2,'second',2.20); UPDATE srcsql.pg_items SET id=3 WHERE id=2; DELETE FROM srcsql.pg_items WHERE id=1;"
  assert_pg "SELECT id||':'||amount FROM dst.items" '3:2.20'
  stop
  sql "INSERT INTO srcsql.pg_items VALUES(4,'offline',4.40)"
  start sql-pg
  assert_pg 'SELECT count(*) FROM dst.items' 2
  stop
  start pg-sql
  assert_sql "SELECT CONCAT(name,':',amount) FROM dstsql.pg_items WHERE id=1" 'pg-initial:999.99'
  pg "UPDATE src.items SET amount=1.20 WHERE id=1; INSERT INTO src.items VALUES(2,'second',2.20); UPDATE src.items SET id=3 WHERE id=2; DELETE FROM src.items WHERE id=1;" >/dev/null
  assert_sql "SELECT CONCAT(id,':',amount) FROM dstsql.pg_items" '3:2.20'
  stop
  pg "INSERT INTO src.items VALUES(4,'offline',4.40)" >/dev/null
  start pg-sql
  assert_sql 'SELECT count(*) FROM dstsql.pg_items' 2
  stop
  echo 'MySQL -> PostgreSQL and PostgreSQL -> MySQL snapshots, exact decimals, DML, primary-key changes and offline restart passed.'
  docker rm -fv "$pg_name" >/dev/null
fi

if [[ "$suite" == all || "$suite" == clickhouse ]]; then
  cat >"$work/ch-small.xml" <<'XML'
<clickhouse>
  <max_server_memory_usage>536870912</max_server_memory_usage><max_thread_pool_size>128</max_thread_pool_size>
  <background_schedule_pool_size>16</background_schedule_pool_size><background_pool_size>8</background_pool_size>
  <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <background_move_pool_size>2</background_move_pool_size><background_fetches_pool_size>2</background_fetches_pool_size><background_common_pool_size>2</background_common_pool_size>
  <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
</clickhouse>
XML
  chmod 644 "$work/ch-small.xml"
  docker run -d --name "$ch_name" --memory=768m --cpus=1 -p 127.0.0.1::8123 \
    -e CLICKHOUSE_USER=cdc -e CLICKHOUSE_PASSWORD="$password" -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    --mount "type=bind,source=$work/ch-small.xml,target=/etc/clickhouse-server/config.d/cdc-test.xml,readonly" clickhouse/clickhouse-server:25.8.7.3 >/dev/null
  ch() { docker exec "$ch_name" clickhouse-client --user cdc --password "$password" --multiquery --query "$1"; }
  ready=false
  for i in $(seq 1 90); do if ch 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi; sleep 1; done
  [[ "$ready" == true ]]
  ch_port=$(docker port "$ch_name" 8123/tcp | head -n1 | awk -F: '{print $NF}')
  ch 'SELECT version(); CREATE DATABASE dst; CREATE TABLE dst.states(entity_key String,version UInt64,is_deleted UInt8,event_id String,payload String,source_position String,source_time_ms Int64) ENGINE=ReplacingMergeTree(version,is_deleted) ORDER BY entity_key SETTINGS fsync_after_insert=1,fsync_part_directory=1;'
  sql "CREATE TABLE srcsql.ch_items $definition; INSERT INTO srcsql.ch_items VALUES(1,'snapshot',123.45);"
  python3 - "$work" "$mysql_port" "$ch_port" "$password" <<'PY'
import json,pathlib,sys
folder,sqlport,chport,password=sys.argv[1:]
source=dict(type='MYSQL',options=dict(host='127.0.0.1',port=sqlport,user='root',password=password,database='srcsql',table='ch_items',sslMode='REQUIRED',replicationServerId='47303'))
sink=dict(type='CLICKHOUSE',options=dict(host='127.0.0.1',port=chport,user='cdc',password=password,database='dst',table='states',tls='false',allowInsecure='true',mode='current_state',exclusiveWriter='true'))
task=dict(id='sql-ch',source=source,sink=sink,snapshotMode='initial',mapping=dict(rename={},conversions={},exclude=[]),journalBytes=268435456,journalEvents=1000000,batchSize=128,maxFailures=30)
path=pathlib.Path(folder,'sql-ch.json');path.write_text(json.dumps(task));path.chmod(0o600)
PY
  assert_ch() {
    local query=$1 expected=$2
    for i in $(seq 1 90); do
      if [[ $(ch "$query" 2>/dev/null) == "$expected" ]]; then return; fi
      if ! kill -0 "$pid" 2>/dev/null; then echo 'Cross-task worker exited unexpectedly' >&2; return 1; fi
      sleep 1
    done
    echo 'Cross-task ClickHouse assertion timed out' >&2; return 1
  }
  start sql-ch
  assert_ch "SELECT JSONExtractRaw(payload,'amount') FROM dst.states FINAL WHERE is_deleted=0" '123.45'
  sql "UPDATE srcsql.ch_items SET amount=999.99 WHERE id=1; INSERT INTO srcsql.ch_items VALUES(2,'second',2.20); UPDATE srcsql.ch_items SET id=3 WHERE id=2; DELETE FROM srcsql.ch_items WHERE id=1;"
  assert_ch "SELECT JSONExtractInt(payload,'id') FROM dst.states FINAL WHERE is_deleted=0" 3
  stop
  sql "INSERT INTO srcsql.ch_items VALUES(4,'offline',4.40)"
  start sql-ch
  assert_ch 'SELECT count() FROM dst.states FINAL WHERE is_deleted=0' 2
  stop
  echo 'MySQL -> ClickHouse versioned snapshot, exact decimal payload, DML, tombstones, primary-key change and offline restart passed.'
  docker rm -fv "$ch_name" >/dev/null
fi
echo 'Heterogeneous CDC acceptance completed for the selected fixtures.'
