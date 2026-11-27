"""Run against the exact packaged application and isolated real services."""
import base64,json,os,pathlib,secrets,socket,subprocess,time,urllib.request,urllib.error
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
if kind=="MONGODB": env["CDC_CONTROL_MONGO_URI"]=f'mongodb://root:{secret}@127.0.0.1:{e["CONTROL_TEST_MONGO_PORT"]}/?authSource=admin&directConnection=true'
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
    wait(lambda:all(status(i)=="PAUSED" for i in range(1,count+1)))
    assert request("/audit")
    for i in range(1,count+1):assert (state/"tasks"/f"task-{i}"/"journal.db").exists()
    print(f"{kind}: real metadata CRUD, credential preservation, owner/key checks, {count} simultaneous CDC tasks, crash and resume passed.")
finally:
    stop()
