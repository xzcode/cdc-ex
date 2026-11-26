#!/usr/bin/env bash
# Disposable three-task pipeline: MySQL -> Kafka A -> Kafka B -> MySQL.
set -euo pipefail
jar="$(realpath "${1:-cdc-server/target/cdc-server-0.1.0-SNAPSHOT.jar}")"
work="$(mktemp -d)"
broker="cdc-ex-kafka-$$-$RANDOM"
mysql="cdc-ex-kafka-mysql-$$-$RANDOM"
password="$(openssl rand -hex 18)"
declare -A pids=()
cleanup() {
  rc=$?
  for id in "${!pids[@]}"; do kill -TERM "${pids[$id]}" 2>/dev/null || true; done
  for id in "${!pids[@]}"; do wait "${pids[$id]}" 2>/dev/null || true; done
  if [[ "$rc" != 0 ]]; then
    for file in "$work"/*.log; do [[ ! -f "$file" ]] || tail -n 80 "$file"; done
    docker logs --tail 40 "$broker" 2>/dev/null || true
    docker logs --tail 20 "$mysql" 2>/dev/null || true
  fi
  docker rm -fv "$broker" "$mysql" >/dev/null 2>&1 || true
  rm -rf "$work"
  return "$rc"
}
trap cleanup EXIT
broker_port="$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1',0))
    print(s.getsockname()[1])
PY
)"
cluster_id="$(python3 - <<'PY'
import base64,uuid
print(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))
PY
)"
docker run -d --name "$broker" --memory 768m --cpus 1 -p "127.0.0.1:$broker_port:9092" \
  -e 'KAFKA_HEAP_OPTS=-Xms256m -Xmx384m' \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093 \
  -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:19092,EXTERNAL://127.0.0.1:$broker_port" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e "CLUSTER_ID=$cluster_id" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false apache/kafka:4.0.0 >/dev/null
docker run -d --name "$mysql" --memory 512m --cpus 1 -p 127.0.0.1::3306 \
  -e "MYSQL_ROOT_PASSWORD=$password" -e MYSQL_ROOT_HOST=% mysql:8.4.13 \
  --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
  --innodb-buffer-pool-size=64M --innodb-log-buffer-size=8M --max-connections=32 --performance-schema=OFF >/dev/null
kafka_cli(){ docker exec "$broker" "/opt/kafka/bin/$1" --bootstrap-server localhost:19092 "${@:2}"; }
ready_broker(){
  for i in $(seq 1 90); do if kafka_cli kafka-topics.sh --list >/dev/null 2>&1; then return 0; fi; sleep 1; done
  echo 'Kafka broker did not become ready'; return 1
}
sql(){ docker exec -e "MYSQL_PWD=$password" "$mysql" mysql -uroot -N -B -e "$1"; }
ready_broker
mysql_ready=false
for i in $(seq 1 90); do
  if docker exec -e "MYSQL_PWD=$password" "$mysql" mysql -h127.0.0.1 -uroot -N -B -e 'SELECT 1' >/dev/null 2>&1; then mysql_ready=true; break; fi
  sleep 1
done
[[ "$mysql_ready" == true ]] || { echo 'MySQL did not become ready'; exit 1; }
kafka_cli kafka-topics.sh --create --topic events-a --partitions 3 --replication-factor 1 >/dev/null
kafka_cli kafka-topics.sh --create --topic events-b --partitions 3 --replication-factor 1 >/dev/null
echo "Kafka $(docker exec "$broker" /opt/kafka/bin/kafka-topics.sh --version); MySQL $(sql 'SELECT VERSION()')"
sql 'CREATE DATABASE src; CREATE DATABASE dst; CREATE TABLE src.items(id BIGINT PRIMARY KEY,name VARCHAR(40),amount DECIMAL(18,2)) ENGINE=InnoDB; CREATE TABLE dst.items LIKE src.items; INSERT INTO src.items VALUES(1,"snapshot",123.45);'
mysql_port="$(docker port "$mysql" 3306/tcp | head -1 | sed 's/.*://')"
python3 - "$work" "$mysql_port" "$broker_port" "$password" <<'PY'
import json,pathlib,sys
work,port,broker,password=sys.argv[1:]
def database(name):
    return {'type':'MYSQL','options':{'host':'127.0.0.1','port':port,'user':'root','password':password,'database':name,'table':'items','sslMode':'REQUIRED','replicationServerId':'48137'}}
def kafka(topic,group):
    return {'type':'KAFKA','options':{'bootstrapServers':'127.0.0.1:'+broker,'topic':topic,'groupId':group,'securityProtocol':'PLAINTEXT','allowInsecure':'true','startOffset':'earliest'}}
tasks=[('publish',database('src'),kafka('events-a','cdc-ex-unused-sink')),
       ('forward',kafka('events-a','cdc-ex-forward'),kafka('events-b','cdc-ex-unused-sink-b')),
       ('consume',kafka('events-b','cdc-ex-consume'),database('dst'))]
for name,source,sink in tasks:
    path=pathlib.Path(work)/(name+'.json')
    path.write_text(json.dumps({'id':name,'source':source,'sink':sink,'snapshotMode':'initial','mapping':{'rename':{},'conversions':{},'exclude':[]},'maxFailures':30}))
    path.chmod(0o600)
PY
start(){
  CDC_RUN=true CDC_TASK_FILE="$work/$1.json" CDC_DATA_DIR="$work/$1" java -jar "$jar" >>"$work/$1.log" 2>&1 &
  pids[$1]=$!
}
stop(){
  if [[ -n "${pids[$1]:-}" ]]; then kill -TERM "${pids[$1]}" 2>/dev/null || true; wait "${pids[$1]}" 2>/dev/null || true; unset 'pids[$1]'; fi
}
assert_sql(){
  local query="$1" expected="$2" actual=""
  for i in $(seq 1 90); do
    for id in "${!pids[@]}"; do kill -0 "${pids[$id]}" 2>/dev/null || { echo "Pipeline task exited: $id"; return 1; }; done
    actual="$(sql "$query")"
    [[ "$actual" != "$expected" ]] || return 0
    sleep 1
  done
  echo "Expected $expected, got $actual"; return 1
}
start consume
start forward
start publish
assert_sql 'SELECT CONCAT(name,":",amount) FROM dst.items WHERE id=1' 'snapshot:123.45'
sql 'INSERT INTO src.items VALUES(2,"inserted",22.10); UPDATE src.items SET amount=999.99 WHERE id=1;'
assert_sql 'SELECT amount FROM dst.items WHERE id=1' '999.99'
assert_sql 'SELECT name FROM dst.items WHERE id=2' 'inserted'
sql 'UPDATE src.items SET id=3 WHERE id=2; DELETE FROM src.items WHERE id=1;'
assert_sql 'SELECT GROUP_CONCAT(id ORDER BY id) FROM dst.items' '3'
stop consume
sql 'UPDATE src.items SET name="after-restart" WHERE id=3; INSERT INTO src.items VALUES(4,"offline",4.10);'
start consume
assert_sql 'SELECT name FROM dst.items WHERE id=3' 'after-restart'
assert_sql 'SELECT amount FROM dst.items WHERE id=4' '4.10'
sql 'RENAME TABLE dst.items TO dst.items_hold; INSERT INTO src.items VALUES(5,"retry",5.25);'
sleep 2
sql 'RENAME TABLE dst.items_hold TO dst.items;'
assert_sql 'SELECT name FROM dst.items WHERE id=5' 'retry'
docker stop -t 2 "$broker" >/dev/null
sql 'INSERT INTO src.items VALUES(6,"broker-recovery",6.25);'
sleep 2
docker start "$broker" >/dev/null
ready_broker
for id in publish forward consume; do
  if ! kill -0 "${pids[$id]}" 2>/dev/null; then wait "${pids[$id]}" 2>/dev/null || true; start "$id"; fi
done
assert_sql 'SELECT name FROM dst.items WHERE id=6' 'broker-recovery'
stop publish
stop forward
sleep 1
before="$(sha256sum "$work/consume/kafka-source.json" | cut -d' ' -f1)"
printf '%s\n' '{broken-envelope}' | docker exec -i "$broker" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic events-b >/dev/null 2>&1
blocked=false
for i in $(seq 1 45); do if ! kill -0 "${pids[consume]}" 2>/dev/null; then blocked=true; break; fi; sleep 1; done
[[ "$blocked" == true ]] || { echo 'Invalid envelope did not block capture'; exit 1; }
if wait "${pids[consume]}"; then echo 'Invalid envelope was incorrectly accepted'; exit 1; fi
unset 'pids[consume]'
[[ -f "$work/consume/kafka-blocked.json" ]] || { echo 'Missing blocked Kafka record audit'; exit 1; }
after="$(sha256sum "$work/consume/kafka-source.json" | cut -d' ' -f1)"
[[ "$before" == "$after" ]] || { echo 'Invalid envelope advanced source offsets'; exit 1; }
if [[ "${CDC_TEST_KAFKA_BROKER_SUITE:-false}" == true ]]; then
  CDC_TEST_KAFKA_BOOTSTRAP="127.0.0.1:$broker_port" "${MVN:-mvn}" -B -ntp -pl cdc-kafka -am test
fi
echo 'MySQL -> Kafka -> Kafka -> MySQL snapshot/DML/key-change/restart/retry/broker-recovery/invalid-envelope checks passed.'
