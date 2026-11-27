#!/usr/bin/env bash
# Authenticated API, encrypted configuration and exclusive process ownership.
# Set CDC_TEST_SECURITY_DATABASE=true to include a disposable real MySQL task through the API.
set -euo pipefail
jar="$(realpath "${1:-cdc-server/target/cdc-server-$(cat VERSION).jar}")"
work="$(mktemp -d)"
chmod 700 "$work"
state="$work/state"
token="$(openssl rand -hex 32)"
secret="cdc-secret-$(openssl rand -hex 24)"
master="$(openssl rand -base64 32)"
pid="" probe_pid="" mysql=""
port="$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1',0)); print(s.getsockname()[1])
PY
)"
base="http://127.0.0.1:$port"
cleanup(){
  rc=$?
  for process in "$pid" "$probe_pid"; do [[ -z "$process" ]] || kill -TERM "$process" 2>/dev/null || true; done
  for process in "$pid" "$probe_pid"; do [[ -z "$process" ]] || wait "$process" 2>/dev/null || true; done
  if [[ "$rc" != 0 ]]; then
    python3 - "$work" "$token" "$secret" "$master" <<'PY'
import pathlib,sys
for path in pathlib.Path(sys.argv[1]).glob('*.log'):
    text='\n'.join(path.read_text(errors='replace').splitlines()[-60:])
    for value in sys.argv[2:]: text=text.replace(value,'[redacted]')
    print(path.name+'\n'+text)
PY
  fi
  [[ -z "$mysql" ]] || docker rm -fv "$mysql" >/dev/null 2>&1 || true
  rm -rf "$work"
  return "$rc"
}
trap cleanup EXIT
start_server(){
  CDC_MODE=server CDC_DATA_DIR="$state" CDC_PORT="$port" CDC_ADMIN_TOKEN="$token" CDC_MASTER_KEY="$master" \
    java -jar "$jar" >>"$work/server.log" 2>&1 & pid=$!
  for i in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo 'Server exited during startup'; return 1; }
    if curl --max-time 2 -fsS "$base/api/health" >"$work/health.json" 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo 'Server health endpoint did not become ready'; return 1
}
stop_server(){
  kill -TERM "$pid"
  for i in $(seq 1 60); do if ! kill -0 "$pid" 2>/dev/null; then wait "$pid" 2>/dev/null || true; pid=""; return 0; fi; sleep 1; done
  kill -KILL "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; pid=""
  echo 'Server did not stop gracefully'; return 1
}
request(){
  local method="$1" path="$2" body="${3:-}" status
  if [[ -n "$body" ]]; then
    status="$(curl --max-time 25 -sS -o "$work/response.json" -w '%{http_code}' -H "Authorization: Bearer $token" -H 'Content-Type: application/json' -X "$method" --data-binary "@$body" "$base$path")"
  else
    status="$(curl --max-time 25 -sS -o "$work/response.json" -w '%{http_code}' -H "Authorization: Bearer $token" -X "$method" "$base$path")"
  fi
  [[ "$status" == 2* ]] || { echo "API request failed: $method $path (HTTP $status)"; return 1; }
}
assert_redacted(){
  python3 - "$1" "$secret" "$token" "$master" <<'PY'
import pathlib,sys
data=pathlib.Path(sys.argv[1]).read_bytes()
if any(s.encode() in data for s in sys.argv[2:]): raise SystemExit('Sensitive value appeared in API response, metadata or logs')
PY
}
assert_owned_probe_fails(){
  local waited=false
  for i in $(seq 1 30); do if ! kill -0 "$probe_pid" 2>/dev/null; then waited=true; break; fi; sleep 1; done
  [[ "$waited" == true ]] || { echo 'Unsafe server startup was not rejected'; return 1; }
  if wait "$probe_pid"; then echo 'Unsafe server startup incorrectly succeeded'; probe_pid=""; return 1; fi
  probe_pid=""
}
# Remote binding must be rejected before a listener starts without the explicit opt-in.
CDC_MODE=server CDC_DATA_DIR="$work/remote-rejected" CDC_BIND=0.0.0.0 CDC_ALLOW_REMOTE=false CDC_PORT="$port" CDC_ADMIN_TOKEN="$token" CDC_MASTER_KEY="$master" \
  java -jar "$jar" >"$work/remote-bind.log" 2>&1 & probe_pid=$!
assert_owned_probe_fails
start_server
assert_redacted "$work/health.json"
status="$(curl --max-time 10 -sS -o "$work/unauthorized.json" -w '%{http_code}' "$base/api/connections")"
[[ "$status" == 401 ]] || { echo "Unauthenticated read returned HTTP $status"; exit 1; }
status="$(curl --max-time 10 -sS -o "$work/unauthorized.json" -w '%{http_code}' -H 'Authorization: Bearer incorrect-token' "$base/api/connectors")"
[[ "$status" == 401 ]] || { echo "Wrong-token read returned HTTP $status"; exit 1; }
request GET /api/connectors
python3 - "$work/response.json" <<'PY'
import json,sys
text=json.dumps(json.load(open(sys.argv[1])))
for name in ['MYSQL','MARIADB','MONGODB','POSTGRESQL','CLICKHOUSE','KAFKA']:
    if name not in text: raise SystemExit('Missing connector in authenticated registry: '+name)
PY
python3 - "$work/probe.json" "$secret" <<'PY'
import json,pathlib,sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({'id':'security-probe','name':'Security probe','endpoint':{'type':'MYSQL','options':{
    'host':'127.0.0.1','port':'9','database':'test','table':'items','user':'probe','password':sys.argv[2],'sslMode':'VERIFY_IDENTITY','replicationServerId':'48901'}}}))
PY
status="$(curl --max-time 10 -sS -o "$work/unauthorized.json" -w '%{http_code}' -H 'Content-Type: application/json' -X POST --data-binary "@$work/probe.json" "$base/api/connections")"
[[ "$status" == 401 ]] || { echo "Unauthenticated write returned HTTP $status"; exit 1; }
request POST /api/connections "$work/probe.json"
assert_redacted "$work/response.json"
request GET /api/connections
assert_redacted "$work/response.json"
python3 - "$work/response.json" <<'PY'
import json,sys
if 'security-probe' not in json.dumps(json.load(open(sys.argv[1]))): raise SystemExit('Created connection missing')
PY
# Malformed bodies must not be echoed in error diagnostics.
printf '{"secret":"%s",invalid}' "$secret" >"$work/malformed.json"
status="$(curl --max-time 10 -sS -o "$work/error.json" -w '%{http_code}' -H "Authorization: Bearer $token" -H 'Content-Type: application/json' -X POST --data-binary "@$work/malformed.json" "$base/api/connections")"
[[ "$status" == 4* ]] || { echo "Malformed JSON returned HTTP $status"; exit 1; }
assert_redacted "$work/error.json"
# The API must not allow a foreign origin to read authenticated responses.
curl --max-time 10 -sS -D "$work/cors-headers.txt" -o "$work/cors.json" -H "Authorization: Bearer $token" -H 'Origin: https://untrusted.invalid' "$base/api/connections" >/dev/null
python3 - "$work/cors-headers.txt" <<'PY'
import pathlib,sys
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if line.lower().startswith('access-control-allow-origin:') and line.split(':',1)[1].strip() in ('*','https://untrusted.invalid'):
        raise SystemExit('Unsafe cross-origin API policy')
PY
assert_redacted "$work/cors.json"
# A second process must never share this metadata and task directory.
probe_port="$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1',0)); print(s.getsockname()[1])
PY
)"
CDC_MODE=server CDC_DATA_DIR="$state" CDC_PORT="$probe_port" CDC_ADMIN_TOKEN="$token" CDC_MASTER_KEY="$master" \
  java -jar "$jar" >"$work/ownership.log" 2>&1 & probe_pid=$!
assert_owned_probe_fails

if [[ "${CDC_TEST_SECURITY_DATABASE:-false}" == true ]]; then
  mysql="cdc-ex-security-mysql-$$-$RANDOM"
  docker run -d --name "$mysql" --memory 512m --cpus 1 -p 127.0.0.1::3306 -e "MYSQL_ROOT_PASSWORD=$secret" -e MYSQL_ROOT_HOST=% mysql:8.4.13 \
    --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
    --innodb-buffer-pool-size=64M --innodb-log-buffer-size=8M --max-connections=32 --performance-schema=OFF >/dev/null
  sql(){ docker exec -e "MYSQL_PWD=$secret" "$mysql" mysql -uroot -N -B -e "$1"; }
  ready=false
  for i in $(seq 1 90); do if docker exec -e "MYSQL_PWD=$secret" "$mysql" mysql -h127.0.0.1 -uroot -N -B -e 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi; sleep 1; done
  [[ "$ready" == true ]] || { echo 'Security test database did not become ready'; exit 1; }
  sql 'CREATE DATABASE src; CREATE DATABASE dst; CREATE TABLE src.items(id BIGINT PRIMARY KEY,name VARCHAR(40)) ENGINE=InnoDB; CREATE TABLE dst.items LIKE src.items; INSERT INTO src.items VALUES(1,"secured-snapshot");'
  db_port="$(docker port "$mysql" 3306/tcp | head -1 | sed 's/.*://')"
  python3 - "$work" "$db_port" "$secret" <<'PY'
import json,pathlib,sys
work,port,password=sys.argv[1:]
for name,database in [('sql-source','src'),('sql-sink','dst')]:
    pathlib.Path(work,name+'.json').write_text(json.dumps({'id':name,'name':name,'endpoint':{'type':'MYSQL','options':{'host':'127.0.0.1','port':port,'database':database,'table':'items','user':'root','password':password,'sslMode':'REQUIRED','replicationServerId':'48903'}}}))
pathlib.Path(work,'task.json').write_text(json.dumps({'id':'secured-task','name':'Secured replication','sourceConnectionId':'sql-source','sinkConnectionId':'sql-sink','snapshotMode':'initial','mapping':{'rename':{},'conversions':{},'exclude':[]}}))
PY
  request POST /api/connections "$work/sql-source.json"
  request POST /api/connections "$work/sql-sink.json"
  request POST '/api/connections/sql-source/test?role=SOURCE'
  assert_redacted "$work/response.json"
  request POST '/api/connections/sql-sink/test?role=SINK'
  request POST /api/tasks "$work/task.json"
  request POST /api/tasks/secured-task/start
  wait_task_state(){
    local expected="$1"
    for i in $(seq 1 60); do
      request GET /api/tasks
      if python3 - "$work/response.json" "$expected" <<'PY'
import json,sys
def find(value):
    if isinstance(value,dict):
        if value.get('id')=='secured-task' and value.get('state')==sys.argv[2]: return True
        return any(find(v) for v in value.values())
    return isinstance(value,list) and any(find(v) for v in value)
raise SystemExit(0 if find(json.load(open(sys.argv[1]))) else 1)
PY
      then return 0; fi
      sleep 1
    done
    echo "Task did not reach $expected"; return 1
  }
  assert_sql(){
    local expected="$1" actual=""
    for i in $(seq 1 90); do actual="$(sql 'SELECT name FROM dst.items WHERE id=1')"; [[ "$actual" != "$expected" ]] || return 0; sleep 1; done
    echo "API task did not deliver expected state: $expected"; return 1
  }
  assert_sql secured-snapshot
  sql 'UPDATE src.items SET name="secured-update" WHERE id=1;'
  assert_sql secured-update
  request POST /api/tasks/secured-task/pause
  wait_task_state PAUSED
  sql 'UPDATE src.items SET name="secured-resume" WHERE id=1;'
  request POST /api/tasks/secured-task/start
  assert_sql secured-resume
  request POST /api/tasks/secured-task/stop
  wait_task_state STOPPED
  request GET /api/tasks
  assert_redacted "$work/response.json"
  echo "Authenticated API MySQL snapshot/update/pause/resume passed: $(sql 'SELECT VERSION()')"
  if [[ "${CDC_TEST_UI:-false}" == true ]]; then
    sql 'CREATE DATABASE ui_src; CREATE DATABASE ui_dst; CREATE DATABASE ui_dst2; CREATE TABLE ui_src.items(id BIGINT PRIMARY KEY,name VARCHAR(40),amount DECIMAL(18,2)) ENGINE=InnoDB; CREATE TABLE ui_dst.items LIKE ui_src.items; CREATE TABLE ui_dst2.items LIKE ui_src.items; INSERT INTO ui_src.items VALUES(1,"ui-snapshot",12.30);'
    CDC_UI_BASE_URL="$base" CDC_UI_TOKEN="$token" CDC_UI_DB_PORT="$db_port" CDC_UI_DB_USER=root CDC_UI_DB_PASSWORD="$secret" \
      CDC_UI_SOURCE_DATABASE=ui_src CDC_UI_TARGET_DATABASE=ui_dst CDC_UI_SECOND_TARGET_DATABASE=ui_dst2 CDC_UI_TABLE=items node scripts/ui-smoke.mjs
  fi
fi

stop_server
# Search persisted metadata, WAL/checkpoints and normal application logs, never the input fixtures.
python3 - "$state" "$work/server.log" "$secret" "$token" "$master" <<'PY'
import pathlib,sys
root=pathlib.Path(sys.argv[1]); files=[p for p in root.rglob('*') if p.is_file() and not p.is_symlink()]+[pathlib.Path(sys.argv[2])]
for path in files:
    data=path.read_bytes()
    if any(v.encode() in data for v in sys.argv[3:]): raise SystemExit('Sensitive configuration persisted unencrypted or logged')
if root.stat().st_mode & 0o077: raise SystemExit('Data directory permits group or other access')
PY
wrong_master="$(openssl rand -base64 32)"
CDC_MODE=server CDC_DATA_DIR="$state" CDC_PORT="$port" CDC_ADMIN_TOKEN="$token" CDC_MASTER_KEY="$wrong_master" \
  java -jar "$jar" >"$work/wrong-key.log" 2>&1 & probe_pid=$!
assert_owned_probe_fails
printf '%s\n' "$master" >"$work/master.key"
chmod 600 "$work/master.key"
CDC_MASTER_KEY_FILE="$work/master.key" start_server
request GET /api/connections
assert_redacted "$work/response.json"
python3 - "$work/response.json" <<'PY'
import json,sys
if 'security-probe' not in json.dumps(json.load(open(sys.argv[1]))): raise SystemExit('Encrypted connection did not survive restart')
PY
stop_server
echo 'Authentication, redaction, encrypted persistence, ownership lock, key verification, remote-bind policy and key-file restart checks passed.'
