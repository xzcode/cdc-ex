#!/usr/bin/env bash
set -euo pipefail
jar=$(realpath "${1:-cdc-server/target/cdc-server-0.1.0-rc.1.jar}")
work=$(mktemp -d)
name="cdcex-mongo-$RANDOM-$$"
pid=''
cleanup() {
  result=$?
  if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if [[ $result -ne 0 ]]; then tail -n 100 "$work/worker.log" 2>/dev/null || true; fi
  docker rm -fv "$name" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$result"
}
trap cleanup EXIT
docker run -d --name "$name" --memory=768m --cpus=1 -p 127.0.0.1::27017 mongo:8.0.15 --replSet rs0 --bind_ip_all --wiredTigerCacheSizeGB=0.25 >/dev/null
port=$(docker port "$name" 27017/tcp | head -n1 | awk -F: '{print $NF}')
mongo() { docker exec "$name" mongosh --quiet --eval "$1"; }
for i in $(seq 1 60); do
  if mongo 'db.adminCommand({ping:1}).ok' >/dev/null 2>&1; then break; fi
  sleep 1
done
mongo 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})' >/dev/null
for i in $(seq 1 60); do
  if [[ $(mongo 'db.hello().isWritablePrimary' 2>/dev/null) == true ]]; then break; fi
  sleep 1
done
mongo 'db=db.getSiblingDB("src");db.createCollection("items",{changeStreamPreAndPostImages:{enabled:true}});db.items.insertOne({_id:ObjectId("507f1f77bcf86cd799439011"),name:"initial",amount:NumberDecimal("12345678901234567890.1234"),long:NumberLong("9007199254740993"),payload:BinData(0,"AAH/"),date:ISODate("2025-01-01T00:00:00.123Z"),nested:{values:[1,null,"x"]}});db=db.getSiblingDB("dst");db.createCollection("items");print(db.version());' 
cat >"$work/task.json" <<JSON
{"id":"mongodb-smoke","source":{"type":"MONGODB","options":{"host":"127.0.0.1","port":"$port","database":"src","table":"items","tls":"false","allowInsecure":"true","directConnection":"true","replicaSet":"rs0"}},"sink":{"type":"MONGODB","options":{"host":"127.0.0.1","port":"$port","database":"dst","table":"items","tls":"false","allowInsecure":"true","directConnection":"true","replicaSet":"rs0","documentMode":"replace"}},"snapshotMode":"initial","mapping":{"rename":{},"conversions":{},"exclude":[]},"journalBytes":268435456,"journalEvents":1000000,"batchSize":128,"maxFailures":40}
JSON
start() { CDC_RUN=true CDC_TASK_FILE="$work/task.json" CDC_DATA_DIR="$work/state" java -jar "$jar" >>"$work/worker.log" 2>&1 & pid=$!; }
stop() { kill -TERM "$pid"; wait "$pid" || [[ $? == 143 ]]; pid=''; }
assert_mongo() {
  local query=$1
  for i in $(seq 1 90); do
    if [[ $(mongo "$query" 2>/dev/null) == true ]]; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then echo 'MongoDB worker exited unexpectedly' >&2; return 1; fi
    sleep 1
  done
  echo 'MongoDB result assertion timed out' >&2
  return 1
}
start
assert_mongo 'const s=db.getSiblingDB("src").items.findOne();const t=db.getSiblingDB("dst").items.findOne();print(t!==null&&EJSON.stringify(s,{relaxed:false})===EJSON.stringify(t,{relaxed:false}))'
mongo 'db=db.getSiblingDB("src");db.items.updateOne({_id:ObjectId("507f1f77bcf86cd799439011")},{$set:{name:"updated","nested.values":[2,"y"]},$unset:{payload:""}});db.items.insertOne({_id:"two",name:"second"});' >/dev/null
assert_mongo 'const d=db.getSiblingDB("dst");print(d.items.countDocuments({})===2&&d.items.findOne({_id:ObjectId("507f1f77bcf86cd799439011")}).name==="updated"&&!Object.hasOwn(d.items.findOne({_id:ObjectId("507f1f77bcf86cd799439011")}),"payload"))'
mongo 'db.getSiblingDB("src").items.deleteOne({_id:"two"})' >/dev/null
assert_mongo 'print(db.getSiblingDB("dst").items.countDocuments({})===1)'
stop
mongo 'db.getSiblingDB("src").items.insertOne({_id:"offline",name:"restart"})' >/dev/null
start
assert_mongo 'print(db.getSiblingDB("dst").items.findOne({_id:"offline"})?.name==="restart")'
# Hard process death exercises replay from the durable Mongo resume token.
kill -KILL "$pid"; wait "$pid" 2>/dev/null || true; pid=''
mongo 'db.getSiblingDB("src").items.updateOne({_id:"offline"},{$set:{name:"after-crash"}})' >/dev/null
start
assert_mongo 'print(db.getSiblingDB("dst").items.findOne({_id:"offline"})?.name==="after-crash")'
# A target outage must retain the pending event; restoring the original UUID lets it retry.
mongo 'db.getSiblingDB("dst").items.renameCollection("items_hold")' >/dev/null
mongo 'db.getSiblingDB("src").items.insertOne({_id:"retry",name:"retained"})' >/dev/null
sleep 2
mongo 'db.getSiblingDB("dst").items_hold.renameCollection("items")' >/dev/null
assert_mongo 'print(db.getSiblingDB("dst").items.findOne({_id:"retry"})?.name==="retained")'
stop
# A fabricated expired boundary must fail closed and must not be replaced with "now".
checkpoint=$(python3 - "$work/state" <<'PY'
import pathlib,sys
paths=list(pathlib.Path(sys.argv[1]).rglob('mongodb-checkpoint.json'))
if len(paths)!=1: raise SystemExit('Expected one MongoDB checkpoint')
print(paths[0])
PY
)
python3 - "$checkpoint" <<'PY'
import hashlib,json,sys
p=sys.argv[1]
with open(p) as f: saved=json.load(f)
state=saved['state']
state.pop('token',None)
state['boundary']={'$timestamp':{'t':1,'i':1}}
# This deliberately constructs a valid but expired offset, rather than a damaged checkpoint.
saved['sha256']=hashlib.sha256(json.dumps(state,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
with open(p,'w') as f: json.dump(saved,f)
PY
before=$(sha256sum "$checkpoint" | cut -d' ' -f1)
start
exited=false
for i in $(seq 1 45); do
  if ! kill -0 "$pid" 2>/dev/null; then exited=true; break; fi
  sleep 1
done
if [[ "$exited" != true ]]; then echo 'Expired MongoDB boundary did not block' >&2; exit 1; fi
if wait "$pid"; then echo 'Expired MongoDB boundary unexpectedly succeeded' >&2; exit 1; fi
pid=''
[[ "$before" == "$(sha256sum "$checkpoint" | cut -d' ' -f1)" ]]
echo 'MongoDB snapshot, exact update images, delete, BSON fidelity, graceful restart, hard crash and retained target retry passed.'
