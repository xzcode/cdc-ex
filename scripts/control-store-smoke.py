"""Run against the exact packaged application and isolated real services."""
import base64,hashlib,json,os,pathlib,secrets,socket,subprocess,time,urllib.request,urllib.error
e=os.environ
root=pathlib.Path(e["CONTROL_TEST_WORK"])
kind=e["CDC_TEST_CONTROL_TYPE"]
secret=e["CONTROL_TEST_SECRET"]
key=base64.b64encode(secrets.token_bytes(32)).decode()
token=secrets.token_hex(32)
count=int(e["CONTROL_TEST_COUNT"])
state=root/"state"
def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1",0));return s.getsockname()[1]
p=port()
env=dict(e,CDC_MODE="server",CDC_DATA_DIR=str(state),CDC_PORT=str(p),CDC_ADMIN_TOKEN=token,CDC_MASTER_KEY=key,
         CDC_CONTROL_DB_TYPE=kind,CDC_CONTROL_ALLOW_INSECURE="true",
         CDC_CONTROL_MYSQL_HOST="127.0.0.1",CDC_CONTROL_MYSQL_PORT=e["CONTROL_TEST_SQL_PORT"],
         CDC_CONTROL_MYSQL_DATABASE="cdc_control",CDC_CONTROL_MYSQL_USER="root",CDC_CONTROL_MYSQL_PASSWORD=secret,CDC_CONTROL_MYSQL_SSL_MODE="REQUIRED",
         CDC_CONTROL_MONGO_DATABASE="cdc_control",CDC_CONTROL_MONGO_TLS="false")
env.pop("CDC_MASTER_KEY_FILE",None)
if "CONTROL_TEST_MONGO_PORT" in e: env["CDC_CONTROL_MONGO_URI"]=f'mongodb://root:{secret}@127.0.0.1:{e["CONTROL_TEST_MONGO_PORT"]}/?authSource=admin&directConnection=true'
proc=None
def request(path,method="GET",body=None):
    data=json.dumps(body).encode() if body is not None else None
    req=urllib.request.Request(f"http://127.0.0.1:{p}/api"+path,data=data,method=method,headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
    with urllib.request.urlopen(req,timeout=30) as r:
        raw=r.read(); assert secret.encode() not in raw and token.encode() not in raw and key.encode() not in raw
        return json.loads(raw) if raw else None
def wait(check,timeout=100):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            if check():return
        except (urllib.error.URLError,TimeoutError,ConnectionError):pass
        if proc and proc.poll() is not None:raise AssertionError("Server exited")
        time.sleep(.5)
    raise AssertionError("Condition timed out")
def launch(overrides=None,name="server"):
    config=dict(env,**(overrides or {}))
    with (root/(name+".log")).open("ab") as log:
        return subprocess.Popen(["java","-Xmx640m","-XX:MaxDirectMemorySize=96m","-jar",e["CONTROL_TEST_JAR"]],env=config,stdout=log,stderr=subprocess.STDOUT)
def start():
    global proc
    proc=launch();wait(lambda:request("/health")["status"]=="UP")
def stop(kill=False):
    global proc
    if proc:
        if proc.poll() is None:
            proc.kill() if kill else proc.terminate()
            try:proc.wait(timeout=60)
            except subprocess.TimeoutExpired:proc.kill();proc.wait();raise
        proc=None
def reject(overrides,name):
    test=launch(dict(overrides,CDC_PORT=str(port())),name)
    try:assert test.wait(timeout=35)!=0,"Unexpected unsafe startup"
    finally:
        if test.poll() is None:test.kill();test.wait()
def sql(query):
    return subprocess.check_output(["docker","exec","-e","MYSQL_PWD="+secret,e["CONTROL_TEST_SQL_CONTAINER"],"mysql","-h127.0.0.1","-uroot","-NB","-e",query],text=True).strip()
def connection(id,db,table,repl):
    return {"id":id,"name":id,"endpoint":{"type":"MYSQL","options":{"host":"127.0.0.1","port":e["CONTROL_TEST_SQL_PORT"],"database":db,"table":table,"user":"root","password":secret,"sslMode":"REQUIRED","replicationServerId":str(repl)}}}
def status(i):
    return request(f"/tasks/task-{i}")["state"]
def started(i):
    return status(i) in ("CAPTURING","RETRYING")
try:
    start()
    for i in range(1,count+1):
        a=connection(f"source-{i}","src","items",55000+i)
        b=connection(f"target-{i}","dst",f"items_{i}",55000+i)
        request("/connections","POST",a);request("/connections","POST",b)
        # Preserve saved credentials across update without returning plaintext.
        a["name"]="Updated "+a["name"];a["endpoint"]["options"]["password"]=""
        request(f"/connections/source-{i}","PUT",a)
        request("/tasks","POST",{"id":f"task-{i}","name":f"Task {i}","sourceConnectionId":f"source-{i}","sinkConnectionId":f"target-{i}","snapshotMode":"initial","mapping":{"rename":{},"conversions":{},"exclude":[]}})
        request(f"/tasks/task-{i}/start","POST")
        wait(lambda:sql(f"SELECT COUNT(*) FROM dst.items_{i}")== "1")
    assert all(started(i) for i in range(1,count+1)),"Active task count regression"
    sql("UPDATE src.items SET name='streamed' WHERE id=1")
    wait(lambda:all(sql(f"SELECT name FROM dst.items_{i} WHERE id=1")=="streamed" for i in range(1,count+1)))
    assert len(request("/connections"))==2*count and len(request("/tasks"))==count and request("/audit")
    if kind=="MYSQL":
        assert sql("SELECT COUNT(*) FROM cdc_control.cdc_ex_records")==str(3*count)
        assert sql("SELECT COUNT(*) FROM cdc_control.cdc_ex_records WHERE LOCATE(UNHEX('"+secret.encode().hex()+"'),body)>0")=="0"
    elif kind=="MONGODB":
        output=subprocess.check_output(["docker","exec",e["CONTROL_TEST_MONGO_CONTAINER"],"mongosh","--quiet","-u","root","-p",secret,"--authenticationDatabase","admin","--eval","const d=db.getSiblingDB('cdc_control'); print(d.cdc_ex_records.countDocuments()); print(d.cdc_ex_records.findOne({kind:'connection'}).body._bsontype)"],text=True)
        assert output.strip().splitlines()==[str(3*count),"Binary"],"Unexpected stored MongoDB records"
    # Hard crash, not a graceful stop; recover metadata and local checkpoints.
    stop(kill=True)
    reject({"CDC_MASTER_KEY":base64.b64encode(secrets.token_bytes(32)).decode()},"wrong-key")
    if kind!="SQLITE":
        reject({"CDC_DATA_DIR":str(root/"other")},"wrong-owner")
        reject({"CDC_CONTROL_DB_TYPE":"SQLITE"},"wrong-backend")
        reject({"CDC_CONTROL_MYSQL_DATABASE":"cdc_empty","CDC_CONTROL_MONGO_DATABASE":"cdc_empty"},"missing-owner")
        assert not (state/"control.db").exists()
    else:
        reject({"CDC_CONTROL_DB_TYPE":"MYSQL"},"wrong-backend")
    sql("INSERT INTO src.items VALUES(2,'offline')")
    start()
    for i in range(1,count+1):
        assert status(i)=="STOPPED"
        request(f"/tasks/task-{i}/start","POST")
    wait(lambda:all(sql(f"SELECT COUNT(*) FROM dst.items_{i}")== "2" for i in range(1,count+1)))
    assert all(started(i) for i in range(1,count+1))
    for i in range(1,count+1):request(f"/tasks/task-{i}/pause","POST")
    def paused_all():
        states=[status(i) for i in range(1,count+1)]
        assert not any(s=="BLOCKED" for s in states), "Graceful pause unexpectedly blocked: "+str(states)
        return all(s=="PAUSED" for s in states)
    wait(paused_all)
    # Verify the stopped generation is actually resumable after the native close.
    sql("INSERT INTO src.items VALUES(3,'after-pause')")
    for i in range(1,count+1):request(f"/tasks/task-{i}/start","POST")
    wait(lambda:all(sql(f"SELECT COUNT(*) FROM dst.items_{i}")== "3" for i in range(1,count+1)))
    for i in range(1,count+1):request(f"/tasks/task-{i}/pause","POST")
    wait(paused_all)
    assert request("/audit")
    for i in range(1,count+1):assert (state/"tasks"/f"task-{i}"/"journal.db").exists()
    if e.get("CDC_TEST_TASK_BATCH")=="true":
        assert kind=="SQLITE"
        source_discovery=request("/connections/source-1/resources?query=batch_")
        assert source_discovery=={"resources":["batch_a","batch_b"],"truncated":False}
        assert request("/connections/target-1/resources?query=batch_&limit=1")=={"resources":["batch_a"],"truncated":True}
        definitions=[{"id":"batch-"+str(i),"name":"Batch "+str(i),"group":"Orders",
                      "sourceConnectionId":"source-1","sinkConnectionId":"target-1","snapshotMode":"initial",
                      "sourceOverrides":{"table":name,"replicationServerId":str(56000+i)},
                      "sinkOverrides":{"table":name}} for i,name in enumerate(["batch_a","batch_b"],1)]
        # Whole structural validation before writes; no credential/host override or duplicated source IDs.
        def rejected(body):
            try:request("/tasks/batch","POST",{"tasks":body})
            except urllib.error.HTTPError as exc:assert exc.code in (400,409)
            else:raise AssertionError("Invalid batch accepted")
        bad=json.loads(json.dumps(definitions));bad[1]["sourceOverrides"]["password"]="override-forbidden";rejected(bad)
        bad=json.loads(json.dumps(definitions));bad[1]["sourceOverrides"]["replicationServerId"]=bad[0]["sourceOverrides"]["replicationServerId"];rejected(bad)
        assert len(request("/tasks"))==1
        created=request("/tasks/batch","POST",{"tasks":definitions})
        assert [r["outcome"] for r in created]==["CREATED","CREATED"]
        assert [r["outcome"] for r in request("/tasks/batch","POST",{"tasks":definitions})]==["UNCHANGED","UNCHANGED"]
        ids=[d["id"] for d in definitions]
        def bulk(action,group=None):
            return request("/tasks/bulk","POST",{"ids":ids,"action":action,"group":group})
        def batch_tasks():
            return [request("/tasks/"+i) for i in ids]
        assert all(r["outcome"]=="REQUESTED" for r in bulk("start"))
        wait(lambda:all(sql("SELECT COUNT(*) FROM dst."+name)=="1" for name in ["batch_a","batch_b"]))
        wait(lambda:all(t["state"]=="CAPTURING" for t in batch_tasks()))
        # Let the first health samples initialize; then deliver real streaming changes.
        wait(lambda:all(t["health"].get("capturedPerSecond") is not None for t in batch_tasks()))
        sql("INSERT INTO src.batch_a VALUES(2,'stream-a'); INSERT INTO src.batch_b VALUES(2,'stream-b')")
        wait(lambda:all(sql("SELECT COUNT(*) FROM dst."+name)=="2" for name in ["batch_a","batch_b"]))
        wait(lambda:all(t["health"].get("lastDeliveryDelayMillis") is not None for t in batch_tasks()))
        # Group updates while running do not change the resolved generation or offsets.
        assert all(r["outcome"]=="UPDATED" for r in bulk("group","Billing"))
        assert all(t["group"]=="Billing" and t["state"]=="CAPTURING" for t in batch_tasks())
        assert all(r["outcome"]=="REQUESTED" for r in bulk("pause"))
        wait(lambda:all(t["state"]=="PAUSED" for t in batch_tasks()))
        stop();start()
        assert all(t["group"]=="Billing" and t["health"]["journal"]["total"]>=2 for t in batch_tasks())
        sql("INSERT INTO src.batch_a VALUES(3,'resume-a'); INSERT INTO src.batch_b VALUES(3,'resume-b')")
        assert all(r["outcome"]=="REQUESTED" for r in bulk("start"))
        wait(lambda:all(sql("SELECT COUNT(*) FROM dst."+name)=="3" for name in ["batch_a","batch_b"]))
        assert all(r["outcome"]=="REQUESTED" for r in bulk("stop"))
        wait(lambda:all(t["state"]=="STOPPED" for t in batch_tasks()))
        print("Batch: real MySQL discovery, two independent tasks from shared connection templates, retry, group, bulk lifecycle, retained counters, live rates and event-age samples passed.")
    if e.get("CDC_TEST_CONTROL_MIGRATION")=="true":
        assert kind=="SQLITE" and count==1
        def local_hashes():
            return {str(x.relative_to(state)):hashlib.sha256(x.read_bytes()).hexdigest()
                    for x in (state/"tasks").rglob("*") if x.is_file()}
        def metadata():
            return (request("/connections"),
                    [{k:v for k,v in t.items() if k!="health"} for t in request("/tasks")],
                    request("/audit"))
        def migrate_env(target,db):
            opts={"CDC_MODE":"migrate-control","CDC_MIGRATE_CONFIRM":"true","CDC_MIGRATE_DB_TYPE":target}
            for k,v in env.items():
                if k.startswith("CDC_CONTROL_") and k!="CDC_CONTROL_DB_TYPE":
                    opts["CDC_MIGRATE_"+k[len("CDC_CONTROL_"):]]=v
            if target=="MYSQL":opts["CDC_MIGRATE_MYSQL_DATABASE"]=db
            elif target=="MONGODB":opts["CDC_MIGRATE_MONGO_DATABASE"]=db
            return opts
        # Six directions with a new dedicated DB for each remote destination.
        # A live service must not be migrated, even when the task is paused.
        reject(migrate_env("MYSQL","migration_mysql_1"),"migration-live")
        sequence=[("MYSQL","migration_mysql_1"),("MONGODB","migration_mongo_2"),
                  ("SQLITE",""),("MONGODB","migration_mongo_4"),
                  ("MYSQL","migration_mysql_5"),("SQLITE","")]
        for n,(target,db) in enumerate(sequence,1):
            before=metadata()
            stop()
            hashes=local_hashes()
            binding=state/"control-store.binding"
            previous=binding.read_bytes() if binding.exists() else None
            opts=migrate_env(target,db)
            reject(dict(opts,CDC_MIGRATE_CONFIRM="false"),"migration-no-confirm")
            if n==5:
                # Attempt to overwrite the already used first MySQL repository.
                reject(migrate_env("MYSQL","migration_mysql_1"),"migration-occupied")
            assert (binding.read_bytes() if binding.exists() else None)==previous
            migration=launch(opts,"migration-"+str(n))
            try: assert migration.wait(timeout=90)==0,"Migration failed"
            finally:
                if migration.poll() is None:migration.kill();migration.wait()
            assert local_hashes()==hashes,"Migration changed local task state"
            assert binding.exists() and binding.read_text().splitlines()[0]==target
            reject({"CDC_CONTROL_DB_TYPE":env["CDC_CONTROL_DB_TYPE"]},"migration-old-backend")
            env["CDC_CONTROL_DB_TYPE"]=target
            if target=="MYSQL":env["CDC_CONTROL_MYSQL_DATABASE"]=db
            elif target=="MONGODB":env["CDC_CONTROL_MONGO_DATABASE"]=db
            start()
            assert metadata()==before,"Metadata changed during migration"
            assert status(1)=="PAUSED"
            sql(f"INSERT INTO src.items VALUES({3+n},'after-migration')")
            request("/tasks/task-1/start","POST")
            wait(lambda:sql("SELECT COUNT(*) FROM dst.items_1")==str(3+n))
            assert sql("SELECT name FROM dst.items_1 WHERE id="+str(3+n))=="after-migration"
            request("/tasks/task-1/pause","POST")
            wait(paused_all)
        assert sql("SELECT COUNT(*) FROM migration_mysql_1.cdc_ex_records")=="3"
        assert sql("SELECT COUNT(*) FROM migration_mysql_5.cdc_ex_records")=="3"
        print("All six offline migration directions: exact metadata, unchanged local journal/checkpoints, resumed real CDC; live/occupied/no-confirm/old-backend rejection passed.")
    print(f"{kind}: real metadata CRUD, credential preservation, owner/key checks, {count} simultaneous CDC tasks, crash and resume passed.")
finally:
    stop()
