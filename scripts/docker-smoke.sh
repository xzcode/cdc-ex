#!/usr/bin/env bash
# Build the repository Dockerfile and test an isolated disposable image/volume.
# An explicit image argument may be reused with CDC_TEST_DOCKER_SKIP_BUILD=true.
set -euo pipefail
umask 077
cd "$(git rev-parse --show-toplevel)"
work=$(mktemp -d)
suffix="$$-$RANDOM"
image="${1:-cdc-ex:smoke-$suffix}"
volume="cdcex-docker-data-$suffix"
first="cdcex-docker-first-$suffix"
second="cdcex-docker-second-$suffix"
wrong="cdcex-docker-wrong-key-$suffix"
token=$(openssl rand -hex 32)
master=$(openssl rand -base64 32)
secret="docker-fixture-$(openssl rand -hex 24)"
built=false
image_owned=false
if [[ $# == 0 ]]; then image_owned=true; fi
cleanup() {
  result=$?
  if [[ "$result" != 0 ]]; then
    for name in "$first" "$second" "$wrong"; do docker logs --tail 80 "$name" >"$work/$name.log" 2>&1 || true; done
    python3 - "$work" "$token" "$master" "$secret" <<'PY'
import pathlib,sys
for file in pathlib.Path(sys.argv[1]).glob('*.log'):
    text=file.read_text(errors='replace')
    for secret in sys.argv[2:]: text=text.replace(secret,'[redacted]')
    print(file.name+'\n'+text)
PY
  fi
  docker rm -fv "$first" "$second" "$wrong" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
  if [[ "$built" == true && "$image_owned" == true ]]; then docker image rm "$image" >/dev/null 2>&1 || true; fi
  rm -rf "$work"
  return "$result"
}
trap cleanup EXIT
if [[ "${CDC_TEST_DOCKER_SKIP_BUILD:-false}" != true ]]; then
  docker build --pull --tag "$image" .
  built=true
else
  [[ $# == 1 ]] || { echo 'An explicit image is required when skipping the Docker build' >&2; exit 1; }
fi
[[ $(docker image inspect --format '{{.Config.User}}' "$image") == '10001:10001' ]] || { echo 'Image does not declare the expected non-root user' >&2; exit 1; }
docker volume create "$volume" >/dev/null
cat >"$work/runtime.env" <<ENV
CDC_MODE=server
CDC_DATA_DIR=/data
CDC_BIND=0.0.0.0
CDC_ALLOW_REMOTE=true
CDC_PORT=8080
CDC_ADMIN_TOKEN=$token
CDC_MASTER_KEY=$master
JAVA_TOOL_OPTIONS=-Xms32m -Xmx256m -XX:MaxDirectMemorySize=64m
ENV
run_container() {
  docker run -d --name "$1" --read-only --memory=768m --cpus=1 --pids-limit=256 \
    --cap-drop ALL --security-opt no-new-privileges:true --tmpfs /tmp:rw,nosuid,nodev,size=64m \
    --mount "type=volume,source=$volume,target=/data" -p 127.0.0.1::8080 \
    --health-interval=2s --health-timeout=3s --health-start-period=5s --health-retries=25 \
    --env-file "$2" "$image" >/dev/null
}
await_health() {
  local name=$1
  for _ in $(seq 1 90); do
    case "$(docker inspect --format '{{.State.Status}}:{{.State.Health.Status}}' "$name")" in
      running:healthy) return;;
      exited:*|dead:*) echo 'Docker server exited before becoming healthy' >&2; return 1;;
    esac
    sleep 1
  done
  echo 'Docker health check did not become ready' >&2; return 1
}
set_base() { base="http://127.0.0.1:$(docker port "$1" 8080/tcp | head -n1 | awk -F: '{print $NF}')"; }
api() {
  local method=$1 path=$2 body=${3:-} code
  local args=(--max-time 20 -sS -o "$work/response.json" -w '%{http_code}' -X "$method" -H "Authorization: Bearer $token")
  if [[ -n "$body" ]]; then args+=(-H 'Content-Type: application/json' --data-binary "@$body"); fi
  code=$(curl "${args[@]}" "$base$path")
  [[ "$code" == 2* ]] || { echo "Docker API returned HTTP $code" >&2; return 1; }
}
assert_connection() {
  api GET /api/connections
  python3 - "$work/response.json" "$secret" <<'PY'
import json,pathlib,sys
text=pathlib.Path(sys.argv[1]).read_text()
rows=json.loads(text)
assert len(rows)==1 and rows[0]['id']=='docker-persisted'
assert rows[0]['name']=='Persistent Docker fixture'
assert rows[0]['options']['database']=='fixture'
assert 'password' in rows[0]['configuredSecrets']
assert sys.argv[2] not in text and 'password' not in rows[0]['options']
PY
}
run_container "$first" "$work/runtime.env"
await_health "$first"
set_base "$first"
docker exec "$first" /app/healthcheck.sh
[[ $(docker exec "$first" id -u) == 10001 ]]
[[ $(docker inspect --format '{{.HostConfig.ReadonlyRootfs}}' "$first") == true ]]
[[ $(docker exec "$first" stat -c '%u:%g:%a' /data) == '10001:10001:700' ]]
[[ $(docker exec "$first" stat -c '%a' /data/control.db) == 600 ]]
if docker exec "$first" sh -c 'touch /app/read-only-probe' >/dev/null 2>&1; then
  echo 'Application directory is writable with a read-only root filesystem' >&2; exit 1
fi
curl --max-time 10 -fsS "$base/api/health" >"$work/health.json"
python3 - "$work/health.json" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))=={'status':'UP','product':'CDC EX'}
PY
[[ $(curl --max-time 10 -sS -o /dev/null -w '%{http_code}' "$base/api/connections") == 401 ]]
curl --max-time 10 -fsS "$base/" >"$work/index.html"
python3 - "$work/index.html" <<'PY'
import pathlib,sys
html=pathlib.Path(sys.argv[1]).read_text()
assert '<div id="app">' in html and '/assets/' in html
PY
python3 - "$work/connection.json" "$secret" <<'PY'
import json,pathlib,sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({'id':'docker-persisted','name':'Persistent Docker fixture',
 'endpoint':{'type':'MYSQL','options':{'host':'127.0.0.1','port':'1','database':'fixture','table':'items',
 'user':'fixture','password':sys.argv[2],'sslMode':'REQUIRED'}}}))
PY
api POST /api/connections "$work/connection.json"
assert_connection
docker stop --time 60 "$first" >/dev/null
docker cp "$first:/data/." "$work/persisted" >/dev/null
docker logs "$first" >"$work/first.log" 2>&1
python3 - "$work/persisted" "$work/first.log" "$token" "$master" "$secret" <<'PY'
import pathlib,sys
files=[p for p in pathlib.Path(sys.argv[1]).rglob('*') if p.is_file()]+[pathlib.Path(sys.argv[2])]
assert any(p.name=='control.db' for p in files)
for file in files:
    data=file.read_bytes()
    assert not any(value.encode() in data for value in sys.argv[3:]),'Sensitive fixture value persisted in plaintext or logs'
PY

# A lost or changed master key cannot silently replace the existing encrypted store.
python3 - "$work/runtime.env" "$work/wrong.env" <<'PY'
import base64,os,pathlib,sys
lines=pathlib.Path(sys.argv[1]).read_text().splitlines()
pathlib.Path(sys.argv[2]).write_text('\n'.join('CDC_MASTER_KEY='+base64.b64encode(os.urandom(32)).decode() if line.startswith('CDC_MASTER_KEY=') else line for line in lines)+'\n')
PY
run_container "$wrong" "$work/wrong.env"
for _ in $(seq 1 60); do
  if [[ $(docker inspect --format '{{.State.Running}}' "$wrong") == false ]]; then break; fi
  sleep 1
done
[[ $(docker inspect --format '{{.State.Running}}' "$wrong") == false ]] || { echo 'Wrong master key did not block startup' >&2; exit 1; }
[[ $(docker inspect --format '{{.State.ExitCode}}' "$wrong") != 0 ]]
run_container "$second" "$work/runtime.env"
await_health "$second"
set_base "$second"
assert_connection
docker exec "$second" /app/healthcheck.sh
docker stop --time 60 "$second" >/dev/null
echo 'Docker build, non-root user, read-only root filesystem, health check, API authentication, encrypted volume persistence and wrong-key rejection passed.'
