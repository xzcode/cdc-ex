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
                if(Objects.equals(candidate.source().options().get("replicationServerId"),other.spec.source().options().get("replicationServerId")))throw new IllegalStateException("Another active task owns this replication client ID");
            }
        }
        Set<String> seen=new HashSet<>();Deque<String> pending=new ArrayDeque<>();pending.add(target);
        while(!pending.isEmpty()){String at=pending.removeFirst();if(at.equals(source))throw new IllegalStateException("Task would create a replication cycle");if(seen.add(at))pending.addAll(graph.getOrDefault(at,List.of()));}
    }
    public synchronized void start(String id)throws Exception{
        if(closing.get()||active.containsKey(id))throw new IllegalStateException("Task is already running or server is stopping");
        Worker worker=new Worker(id,specification(id));active.put(id,worker);
        try{store.state(id,"STARTING",null);store.audit("START_TASK",id,"REQUESTED");executor.submit(worker);}
        catch(Exception e){active.remove(id,worker);try{store.state(id,"STOPPED","START_REQUEST_FAILED");}catch(Exception ignored){}throw e;}
    }
    public synchronized void stop(String id,boolean pause)throws Exception{
        store.get("task",id);Worker worker=active.get(id);
        if(worker==null){store.state(id,pause?"PAUSED":"STOPPED",null);return;}
        worker.pause=pause;worker.stopping.set(true);if(worker.runner!=null)worker.runner.runtime().requestStop();
        store.state(id,pause?"PAUSING":"STOPPING",null);store.audit(pause?"PAUSE_TASK":"STOP_TASK",id,"REQUESTED");
    }
    public Map<String,Object> view(ControlStore.Record record)throws Exception{
        TaskDefinition d=CdcApplication.JSON.readValue(record.json(),TaskDefinition.class);
        Map<String,Object> view=new LinkedHashMap<>();view.put("id",d.id());view.put("name",d.name());view.put("sourceConnectionId",d.sourceConnectionId());view.put("sinkConnectionId",d.sinkConnectionId());view.put("snapshotMode",d.snapshotMode());view.put("mapping",d.mapping());
        view.put("state",record.state());view.put("error",record.error());view.put("updated",record.updated());
        TaskSpec settings=d.resolve(connection(d.sourceConnectionId()),connection(d.sinkConnectionId()));
        view.put("limits",Map.of("journalBytes",settings.journalBytes(),"journalEvents",settings.journalEvents(),"batchSize",settings.batchSize(),"maxFailures",settings.maxFailures()));
        Worker worker=active.get(d.id());TaskRuntime.Health health=completed.get(d.id());
        if(worker!=null&&worker.runner!=null){try{health=worker.runner.runtime().health();if(!worker.stopping.get())view.put("state",health.state());}catch(Exception ignored){}}
        view.put("health",health);return view;
    }
    public List<Map<String,Object>> views()throws Exception{List<Map<String,Object>> result=new ArrayList<>();for(var r:store.list("task"))result.add(view(r));return result;}
    public synchronized void limits(String id,long bytes,long events,int batch,int failures)throws Exception{
        if(active.containsKey(id))throw new IllegalStateException("Pause the task before changing resource limits");
        TaskDefinition old=definition(id);TaskSpec current=specification(id);
        var next=new TaskDefinition(id,old.name(),old.sourceConnectionId(),old.sinkConnectionId(),old.snapshotMode(),old.mapping(),
            bytes==0?current.journalBytes():bytes,events==0?current.journalEvents():events,batch==0?current.batchSize():batch,failures==0?current.maxFailures():failures);
        TaskSpec validated=next.resolve(current.source(),current.sink());Path journal=taskDirectory(id).resolve("journal.db");
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
                if(stopping.get())runner.runtime().requestStop();store.state(id,"CAPTURING",null);runner.run();state=pause?"PAUSED":"STOPPED";
            }catch(Exception e){state="BLOCKED";var failure=Failure.describe(e);error=failure.code()+": "+failure.message();System.err.println("Task "+id+" blocked ["+failure.code()+"] at "+failure.location());}
            finally{
                if(runner!=null){try{completed.put(id,runner.runtime().health());}catch(Exception ignored){}try{runner.close();}catch(Exception e){state="BLOCKED";error="STATE_CLOSE_FAILED";}}
                try{store.state(id,stopping.get()&&error==null?(pause?"PAUSED":"STOPPED"):state,error);store.audit("TASK_FINISHED",id,error==null?state:error);}catch(Exception ignored){}
                active.remove(id,this);
            }
        }
    }
    @Override public void close()throws Exception{
        closing.set(true);for(String id:List.copyOf(active.keySet()))stop(id,true);executor.shutdown();
        if(!executor.awaitTermination(45,TimeUnit.SECONDS)){executor.shutdownNow();throw new IllegalStateException("Task shutdown timeout; recovery inspection required");}
    }
}
