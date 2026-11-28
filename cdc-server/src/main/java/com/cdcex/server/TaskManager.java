package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.runtime.*;
import com.cdcex.storage.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TaskManager implements AutoCloseable {
    private final ControlRepository store;private final ConnectorRegistry registry;private final Path directory;
    private final ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String,Worker> active=new ConcurrentHashMap<>();
    private final Map<String,TaskRuntime.Health> completed=new ConcurrentHashMap<>();
    private final AtomicBoolean closing=new AtomicBoolean();
    public TaskManager(ControlRepository store,ConnectorRegistry registry,Path directory)throws Exception{this.store=store;this.registry=registry;this.directory=directory;store.recoverInterrupted();}
    Endpoint connection(String id)throws Exception{return CdcApplication.JSON.readValue(store.get("connection",id).json(),Endpoint.class);}
    TaskDefinition definition(String id)throws Exception{return CdcApplication.JSON.readValue(store.get("task",id).json(),TaskDefinition.class);}
    TaskSpec specification(String id)throws Exception{TaskDefinition d=definition(id);return d.resolve(connection(d.sourceConnectionId()),connection(d.sinkConnectionId()));}
    Path taskDirectory(String id){ControlStore.id(id);return directory.resolve("tasks").resolve(id);}
    record BatchResult(String id,String outcome,String code){}
    private static BatchResult failure(String id,Exception e){
        return new BatchResult(id,"FAILED",e instanceof NoSuchElementException?"NOT_FOUND":e instanceof IllegalArgumentException?"INVALID_CONFIGURATION":e instanceof IllegalStateException?"CONFLICT":Failure.describe(e).code());
    }
    private TaskSpec checkDefinition(TaskDefinition d)throws Exception{
        TaskSpec spec=d.resolve(connection(d.sourceConnectionId()),connection(d.sinkConnectionId()));
        if(d.sourceConnectionId().equals(d.sinkConnectionId())&&spec.source().equals(spec.sink()))throw new IllegalArgumentException("Source and destination must differ");
        var capabilities=registry.get(spec.source().type()).capabilities().get("sourceModes");
        if(!(capabilities instanceof List<?> modes)||!modes.contains(spec.snapshotMode()))throw new IllegalArgumentException("Unsupported capture mode");
        registry.get(spec.sink().type());
        return spec;
    }
    public synchronized void create(TaskDefinition d)throws Exception{
        checkDefinition(d);
        if(Files.exists(taskDirectory(d.id()),LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Task recovery directory already exists");
        store.create("task",d.id(),d.name(),CdcApplication.JSON.writeValueAsString(d));
    }
    /** Precheck the complete request before any writes. Each durable create is reported separately.
     * Exact retries are safe; a different definition under an existing ID is rejected.
     */
    public synchronized List<BatchResult> createBatch(List<TaskDefinition> definitions)throws Exception{
        if(definitions==null||definitions.isEmpty())throw new IllegalArgumentException("Choose tasks to create");
        Set<String> ids=new HashSet<>(),destinations=new HashSet<>(),clients=new HashSet<>();
        Map<String,TaskDefinition> existing=new HashMap<>();
        for(var row:store.list("task"))existing.put(row.id(),CdcApplication.JSON.readValue(row.json(),TaskDefinition.class));
        for(var d:definitions){
            if(d==null||!ids.add(d.id()))throw new IllegalArgumentException("Duplicate task identifier in batch");
            TaskSpec spec=checkDefinition(d);
            if(existing.containsKey(d.id())&&!existing.get(d.id()).equals(d))throw new IllegalStateException("Task ID already belongs to another definition");
            if(!existing.containsKey(d.id())&&Files.exists(taskDirectory(d.id()),LinkOption.NOFOLLOW_LINKS))throw new IllegalStateException("Task recovery directory already exists");
            String dest=d.sinkConnectionId()+":"+TaskDefinition.resource(spec.sink());
            if(!destinations.add(dest))throw new IllegalArgumentException("Repeated destination in batch");
            Endpoint src=spec.source();String ownerKey=switch(src.type()){
                case "MYSQL","MARIADB"->src.require("replicationServerId");
                case "POSTGRESQL"->src.require("slot");
                case "KAFKA"->src.require("groupId");
                default->"";
            };
            if(!ownerKey.isEmpty()&&!clients.add(d.sourceConnectionId()+":"+ownerKey))throw new IllegalArgumentException("Repeated source replication identity in batch");
        }
        List<BatchResult> result=new ArrayList<>();
        for(var d:definitions){
            if(existing.containsKey(d.id())){result.add(new BatchResult(d.id(),"UNCHANGED",null));continue;}
            try{store.create("task",d.id(),d.name(),CdcApplication.JSON.writeValueAsString(d));result.add(new BatchResult(d.id(),"CREATED",null));}
            catch(Exception e){result.add(failure(d.id(),e));}
        }
        return result;
    }
    public synchronized List<BatchResult> bulk(List<String> ids,String action,String group)throws Exception{
        if(ids==null||ids.isEmpty()||!Set.of("start","pause","stop","group").contains(action))throw new IllegalArgumentException("Invalid bulk action");
        if(action.equals("group"))group=TaskDefinition.normalizeGroup(group);
        Set<String> unique=new LinkedHashSet<>(ids);unique.forEach(ControlStore::id);
        List<BatchResult> result=new ArrayList<>();
        for(String id:unique){
            try{
                TaskDefinition d=definition(id);
                if(action.equals("group")){
                    if(d.group().equals(group)){result.add(new BatchResult(id,"UNCHANGED",null));continue;}
                    var next=d.withGroup(group);store.replace("task",id,next.name(),CdcApplication.JSON.writeValueAsString(next));
                    store.audit("GROUP_TASK",id,"OK");
                }else if(action.equals("start")){
                    if(active.containsKey(id)){result.add(new BatchResult(id,"UNCHANGED",null));continue;}start(id);
                }else{
                    // Do not clear retained errors or convert an inactive task into a paused one.
                    if(!active.containsKey(id)){result.add(new BatchResult(id,"UNCHANGED",null));continue;}stop(id,action.equals("pause"));
                }
                result.add(new BatchResult(id,action.equals("group")?"UPDATED":"REQUESTED",null));
            }catch(Exception e){result.add(failure(id,e));}
        }
        return result;
    }
    public synchronized void connectionIdle(String id)throws Exception{
        for(String task:active.keySet()){TaskDefinition d=definition(task);if(id.equals(d.sourceConnectionId())||id.equals(d.sinkConnectionId()))throw new IllegalStateException("Pause tasks before editing their connection");}
    }
    public synchronized void updateConnection(String id,String name,Endpoint patch)throws Exception{
        connectionIdle(id);Endpoint old=connection(id);
        if(!old.type().equals(patch.type()))throw new IllegalArgumentException("Connector type cannot change");
        Map<String,String> merged=new LinkedHashMap<>(old.options());
        patch.options().forEach((k,v)->{boolean secret=k.toLowerCase(Locale.ROOT).matches(".*(password|secret|token|credential|jaas).*" )||k.equals("uri");if(!secret||!v.isEmpty())merged.put(k,v);});
        store.replace("connection",id,name,CdcApplication.JSON.writeValueAsString(new Endpoint(old.type(),merged)));
    }
    public TaskRunner.Validation validate(String id)throws Exception{return validateSpec(id,specification(id));}
    private TaskRunner.Validation validateSpec(String id,TaskSpec spec)throws Exception{
        TaskRunner.Validation v=TaskRunner.validate(spec,registry,!Files.exists(taskDirectory(id).resolve("journal.db")));
        synchronized(this){guardGraph(id,v);store.identities(id,v.source().identity(),v.sink().identity());}
        store.audit("VALIDATE_TASK",id,"OK");return v;
    }
    private void guardGraph(String id,TaskRunner.Validation v)throws Exception{
        String source=v.source().identity(),target=v.sink().identity();Map<String,List<String>> graph=new HashMap<>();
        for(var r:store.list("task")){
            if(r.id().equals(id)||r.sourceIdentity()==null||r.sinkIdentity()==null)continue;
            graph.computeIfAbsent(r.sourceIdentity(),k->new ArrayList<>()).add(r.sinkIdentity());
            if(active.containsKey(r.id())&&r.sinkIdentity().equals(target))throw new IllegalStateException("Another active task owns this destination");
            Worker other=active.get(r.id());
            if(other!=null&&source.contains(":innodb:")&&r.sourceIdentity().contains(":innodb:")&&source.substring(0,source.indexOf(":innodb:")).equals(r.sourceIdentity().substring(0,r.sourceIdentity().indexOf(":innodb:")))){
                Worker owner=active.get(id);TaskSpec candidate=owner==null?specification(id):owner.spec;
                if(Long.parseLong(candidate.source().require("replicationServerId"))==Long.parseLong(other.spec.source().require("replicationServerId")))throw new IllegalStateException("Another active task owns this replication client ID");
            }
        }
        Set<String> seen=new HashSet<>();Deque<String> pending=new ArrayDeque<>();pending.add(target);
        while(!pending.isEmpty()){String at=pending.removeFirst();if(at.equals(source))throw new IllegalStateException("Task would create a replication cycle");if(seen.add(at))pending.addAll(graph.getOrDefault(at,List.of()));}
    }
    public synchronized void start(String id)throws Exception{
        if(closing.get()||active.containsKey(id))throw new IllegalStateException("Task is already running or server is stopping");
        Worker worker=new Worker(id,specification(id));completed.remove(id);active.put(id,worker);
        try{store.state(id,"STARTING",null);store.audit("START_TASK",id,"REQUESTED");executor.submit(worker);}
        catch(Exception e){active.remove(id,worker);try{store.state(id,"STOPPED","START_REQUEST_FAILED");}catch(Exception ignored){}throw e;}
    }
    public synchronized void stop(String id,boolean pause)throws Exception{
        store.get("task",id);Worker worker=active.get(id);
        if(worker==null){store.state(id,pause?"PAUSED":"STOPPED",null);return;}
        worker.pause=pause;worker.stopping.set(true);if(worker.runner!=null)worker.runner.runtime().requestStop();
        store.state(id,pause?"PAUSING":"STOPPING",null);store.audit(pause?"PAUSE_TASK":"STOP_TASK",id,"REQUESTED");
    }
    public Map<String,Object> view(ControlStore.Record record)throws Exception{return view(record,null);}
    private Map<String,Object> view(ControlStore.Record record,Map<String,Endpoint> endpoints)throws Exception{
        TaskDefinition d=CdcApplication.JSON.readValue(record.json(),TaskDefinition.class);
        Map<String,Object> view=new LinkedHashMap<>();view.put("id",d.id());view.put("name",d.name());view.put("sourceConnectionId",d.sourceConnectionId());view.put("sinkConnectionId",d.sinkConnectionId());view.put("snapshotMode",d.snapshotMode());view.put("mapping",d.mapping());
        view.put("state",record.state());view.put("error",record.error());view.put("updated",record.updated());
        TaskSpec settings=d.resolve(endpoints==null?connection(d.sourceConnectionId()):Objects.requireNonNull(endpoints.get(d.sourceConnectionId())),endpoints==null?connection(d.sinkConnectionId()):Objects.requireNonNull(endpoints.get(d.sinkConnectionId())));
        view.put("group",d.group());view.put("sourceResource",TaskDefinition.resource(settings.source()));view.put("sinkResource",TaskDefinition.resource(settings.sink()));
        view.put("sourceOverrides",d.sourceOverrides());view.put("sinkOverrides",d.sinkOverrides());
        view.put("limits",Map.of("journalBytes",settings.journalBytes(),"journalEvents",settings.journalEvents(),"batchSize",settings.batchSize(),"maxFailures",settings.maxFailures()));
        Worker worker=active.get(d.id());TaskRuntime.Health health=completed.get(d.id());
        if(worker!=null&&worker.runner!=null){try{health=worker.runner.runtime().health();if(!worker.stopping.get())view.put("state",health.state());}catch(Exception ignored){}}
        if(worker==null&&health==null){
            Path journal=taskDirectory(d.id()).resolve("journal.db");
            if(Files.isRegularFile(journal,LinkOption.NOFOLLOW_LINKS)){
                try{
                    health=TaskRuntime.Health.retained(SqliteJournal.readStats(journal));
                    if(!active.containsKey(d.id()))completed.putIfAbsent(d.id(),health);
                }catch(Exception ignored){} // An unavailable journal is unknown, not zero.
            }
        }
        view.put("health",health);return view;
    }
    public List<Map<String,Object>> views()throws Exception{
        Map<String,Endpoint> endpoints=new HashMap<>();
        for(var c:store.list("connection"))endpoints.put(c.id(),CdcApplication.JSON.readValue(c.json(),Endpoint.class));
        List<Map<String,Object>> result=new ArrayList<>();for(var r:store.list("task"))result.add(view(r,endpoints));return result;
    }
    public synchronized void limits(String id,long bytes,long events,int batch,int failures)throws Exception{
        if(active.containsKey(id))throw new IllegalStateException("Pause the task before changing resource limits");
        TaskDefinition old=definition(id);TaskSpec current=specification(id);
        var next=old.withLimits(
            bytes==0?current.journalBytes():bytes,events==0?current.journalEvents():events,batch==0?current.batchSize():batch,failures==0?current.maxFailures():failures);
        TaskSpec validated=next.resolve(connection(next.sourceConnectionId()),connection(next.sinkConnectionId()));Path journal=taskDirectory(id).resolve("journal.db");
        if(Files.exists(journal))try(var j=new SqliteJournal(journal,current.journalBytes(),current.journalEvents())){
            var stats=j.stats();if(validated.journalBytes()<stats.bytes()||validated.journalEvents()<stats.total())throw new IllegalArgumentException("New limits are below retained journal usage");
        }
        store.replace("task",id,next.name(),CdcApplication.JSON.writeValueAsString(next));store.audit("UPDATE_LIMITS",id,"OK");
    }
    public synchronized Map<String,Object> compare(String id,boolean sourceQuiescent,int maxRows)throws Exception{
        if(!sourceQuiescent)throw new IllegalArgumentException("Stop source writes before a consistency comparison");
        if(active.containsKey(id))throw new IllegalStateException("Pause the task after draining its queue before comparison");
        if(maxRows<1||maxRows>100000)throw new IllegalArgumentException("Comparison limit must be between 1 and 100000");
        TaskSpec spec=specification(id);Path journal=taskDirectory(id).resolve("journal.db");
        if(Files.exists(journal))try(var j=new SqliteJournal(journal,spec.journalBytes(),spec.journalEvents())){if(j.stats().pending()!=0)throw new IllegalStateException("Drain all pending events before comparison");}
        var a=registry.get(spec.source().type());var b=registry.get(spec.sink().type());
        var beforeA=a.inspect(spec.source(),ConnectorProvider.Role.SOURCE);var beforeB=b.inspect(spec.sink(),ConnectorProvider.Role.SINK);
        List<Map<String,Object>> source=a.readRows(spec.source(),maxRows+1),target=b.readRows(spec.sink(),maxRows+1);
        if(source.size()>maxRows||target.size()>maxRows)return Map.of("complete",false,"consistent",false,"reason","ROW_LIMIT_EXCEEDED","maxRows",maxRows);
        List<String> keys=beforeB.keyFields();if(keys.isEmpty()&&spec.sink().type().equals("CLICKHOUSE"))keys=beforeA.keyFields().stream().map(spec.mapping()::targetField).toList();
        if(keys.isEmpty())throw new IllegalArgumentException("Comparison requires explicit stable keys");
        boolean bson=spec.sink().type().equals("MONGODB"),ordered=bson||(spec.source().type().equals("MONGODB")&&spec.sink().type().equals("CLICKHOUSE"));
        List<Map<String,Object>> mapped=source.stream().map(row->spec.mapping().row(row,false)).map(row->bson?mongoRow(row):row).toList();
        if(bson)target=target.stream().map(TaskManager::mongoRow).toList();
        Map<String,String> expected=index(mapped,keys,ordered),actual=index(target,keys,ordered);
        List<String> missing=new ArrayList<>(),extra=new ArrayList<>(),different=new ArrayList<>();
        expected.forEach((k,v)->{if(!actual.containsKey(k))missing.add(k);else if(!actual.get(k).equals(v))different.add(k);});
        actual.keySet().forEach(k->{if(!expected.containsKey(k))extra.add(k);});
        var afterA=a.inspect(spec.source(),ConnectorProvider.Role.SOURCE);var afterB=b.inspect(spec.sink(),ConnectorProvider.Role.SINK);
        if(!beforeA.identity().equals(afterA.identity())||!beforeA.schemaFingerprint().equals(afterA.schemaFingerprint())||!beforeB.identity().equals(afterB.identity())||!beforeB.schemaFingerprint().equals(afterB.schemaFingerprint()))throw new IllegalStateException("Resource changed during comparison");
        store.audit("COMPARE_TASK",id,"COMPLETE");
        return Map.of("complete",true,"consistent",missing.isEmpty()&&extra.isEmpty()&&different.isEmpty(),"sourceCount",source.size(),"targetCount",target.size(),
            "missing",missing.size(),"extra",extra.size(),"different",different.size(),"sampleKeyHashes",Map.of("missing",missing.stream().limit(20).toList(),"extra",extra.stream().limit(20).toList(),"different",different.stream().limit(20).toList()),"scope","User-confirmed quiescent source; independent bounded reads, not a distributed snapshot");
    }
    private static Map<String,Object> mongoRow(Map<String,Object> row){
        Map<String,Object> canonical=com.cdcex.mongodb.BsonValues.map(com.cdcex.mongodb.BsonValues.document(row));
        Map<String,Object> out=new LinkedHashMap<>();if(canonical.containsKey("_id"))out.put("_id",canonical.get("_id"));canonical.forEach(out::put);return out;
    }
    private static Map<String,String> index(List<Map<String,Object>> rows,List<String> keys,boolean ordered){
        Map<String,String> result=new HashMap<>();for(var row:rows){Map<String,Object> key=new LinkedHashMap<>();for(String k:keys){if(!row.containsKey(k)||row.get(k)==null)throw new IllegalStateException("Comparison row lacks a key");key.put(k,row.get(k));}
            String digest=EventCodec.digest(normalize(key));if(result.put(digest,ordered?EventCodec.payloadDigest(normalize(row)):EventCodec.digest(normalize(row)))!=null)throw new IllegalStateException("Comparison key is not unique");}return result;
    }
    private static Object normalize(Object value){
        if(value instanceof Number n)return Map.of("$comparisonNumber",new java.math.BigDecimal(n.toString()).stripTrailingZeros().toPlainString());
        if(value instanceof Map<?,?> m){Map<String,Object> out=new LinkedHashMap<>();m.forEach((k,v)->out.put(k.toString(),normalize(v)));return out;}
        if(value instanceof List<?> l)return l.stream().map(TaskManager::normalize).toList();return value;
    }
    private final class Worker implements Runnable{
        final String id;final TaskSpec spec;final AtomicBoolean stopping=new AtomicBoolean();volatile TaskRunner runner;volatile boolean pause;
        Worker(String id,TaskSpec spec){this.id=id;this.spec=spec;}
        public void run(){
            String state="STOPPED",error=null;
            try{
                validateSpec(id,spec);if(stopping.get())return;
                runner=TaskRunner.open(spec,taskDirectory(id),registry);
                synchronized(TaskManager.this){
                    if(stopping.get())runner.runtime().requestStop();
                    else store.state(id,"CAPTURING",null);
                }
                runner.run();state=pause?"PAUSED":"STOPPED";
            }catch(Exception e){state="BLOCKED";var failure=Failure.describe(e);error=failure.code()+": "+failure.message();System.err.println("Task "+id+" blocked ["+failure.code()+"] at "+failure.location());}
            finally{
                if(runner!=null){try{completed.put(id,runner.runtime().health());}catch(Exception ignored){}try{runner.close();}catch(Exception e){state="BLOCKED";error="STATE_CLOSE_FAILED";}}
                // Stop/start and the terminal state transition share the manager lock.
                // A late pause must not overwrite the completed worker with PAUSING.
                synchronized(TaskManager.this){
                    String terminal=stopping.get()&&error==null?(pause?"PAUSED":"STOPPED"):state;
                    try{store.state(id,terminal,error);store.audit("TASK_FINISHED",id,error==null?terminal:error);}catch(Exception ignored){}
                    finally{active.remove(id,this);}
                }
            }
        }
    }
    @Override public void close()throws Exception{
        closing.set(true);for(String id:List.copyOf(active.keySet()))stop(id,true);executor.shutdown();
        if(!executor.awaitTermination(45,TimeUnit.SECONDS)){executor.shutdownNow();throw new IllegalStateException("Task shutdown timeout; recovery inspection required");}
    }
}
