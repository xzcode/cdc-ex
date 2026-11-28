package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.storage.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api")
public final class ControlApi {
    private final ControlRepository store;private final ConnectorRegistry registry;private final TaskManager tasks;
    public ControlApi(ControlRepository store,ConnectorRegistry registry,TaskManager tasks){this.store=store;this.registry=registry;this.tasks=tasks;}
    public record ConnectionInput(String id,String name,Endpoint endpoint){}
    public record BatchCreateInput(List<TaskDefinition> tasks){}
    public record BulkInput(List<String> ids,String action,String group){}
    public record ComparisonInput(boolean sourceQuiescent,int maxRows){}
    public record LimitsInput(long journalBytes,long journalEvents,int batchSize,int maxFailures){}
    @GetMapping("/health") public Map<String,String> health(){return Map.of("status","UP","product","CDC EX");}
    @GetMapping("/connectors") public Object connectors(){return registry.capabilities();}
    @GetMapping("/connections") public Object connections()throws Exception{List<Map<String,Object>> rows=new ArrayList<>();for(var r:store.list("connection"))rows.add(publicConnection(r));return rows;}
    @PostMapping("/connections") @ResponseStatus(HttpStatus.CREATED) public Object connection(@RequestBody ConnectionInput input)throws Exception{
        Objects.requireNonNull(input.endpoint());registry.get(input.endpoint().type());
        store.create("connection",input.id(),input.name(),CdcApplication.JSON.writeValueAsString(input.endpoint()));return publicConnection(store.get("connection",input.id()));
    }
    @PutMapping("/connections/{id}") public Object connection(@PathVariable("id") String id,@RequestBody ConnectionInput input)throws Exception{
        if(!id.equals(input.id()))throw new IllegalArgumentException("Resource ID cannot change");
        tasks.updateConnection(id,input.name(),input.endpoint());return publicConnection(store.get("connection",id));
    }
    private static boolean isSecret(String key){String k=key.toLowerCase(Locale.ROOT);return k.matches(".*(password|secret|token|credential|jaas).*" )||k.equals("uri");}
    private Map<String,Object> publicConnection(ControlStore.Record r)throws Exception{
        Endpoint endpoint=CdcApplication.JSON.readValue(r.json(),Endpoint.class);Map<String,String> safe=new LinkedHashMap<>();List<String> secrets=new ArrayList<>();
        Set<String> visible=Set.of("host","port","database","schema","table","user","sslMode","sslRootCert","tls","allowInsecure","authDatabase","replicaSet","directConnection","replicationServerId","slot","publication","bootstrapServers","topic","groupId","startOffset","securityProtocol","saslMechanism","saslUsername","sslTruststoreLocation","sslTruststoreType","sslKeystoreLocation","sslKeystoreType","documentMode","mode","exclusiveWriter","keyColumns","cursorColumns","cursorGuarantee","startSequence","maxPollRecords","maxMessageBytes","pollIntervalMillis","batchRows");
        endpoint.options().forEach((k,v)->{if(isSecret(k)){if(!v.isEmpty())secrets.add(k);}else if(visible.contains(k))safe.put(k,v);});
        return Map.of("id",r.id(),"name",r.name(),"type",endpoint.type(),"options",safe,"configuredSecrets",secrets,"updated",r.updated());
    }
    @PostMapping("/connections/{id}/test") public Object test(@PathVariable("id") String id,@RequestParam(name="role",defaultValue="SOURCE") ConnectorProvider.Role role)throws Exception{
        Endpoint endpoint=tasks.connection(id);var result=registry.get(endpoint.type()).inspect(endpoint,role);store.audit("TEST_CONNECTION",id,"OK");return result;
    }
    @GetMapping("/connections/{id}/resources") public Object resources(@PathVariable("id") String id,
            @RequestParam(name="query",defaultValue="") String query,@RequestParam(name="limit",defaultValue="500") int limit)throws Exception{
        if(limit<1||limit>2000||query.length()>249)throw new IllegalArgumentException("Invalid discovery query");
        Endpoint endpoint=tasks.connection(id);
        List<String> resources=registry.get(endpoint.type()).resources(endpoint,query,limit+1);
        return Map.of("resources",resources.stream().limit(limit).toList(),"truncated",resources.size()>limit);
    }
    @GetMapping("/tasks") public Object tasks()throws Exception{return tasks.views();}
    @GetMapping("/tasks/{id}") public Object task(@PathVariable("id") String id)throws Exception{return tasks.view(store.get("task",id));}
    @PostMapping("/tasks") @ResponseStatus(HttpStatus.CREATED) public Object task(@RequestBody TaskDefinition definition)throws Exception{
        tasks.create(definition);return task(definition.id());
    }
    @PostMapping("/tasks/batch") @ResponseStatus(HttpStatus.OK) public Object createBatch(@RequestBody BatchCreateInput input)throws Exception{return tasks.createBatch(input.tasks());}
    @PostMapping("/tasks/bulk") public Object bulk(@RequestBody BulkInput input)throws Exception{return tasks.bulk(input.ids(),input.action(),input.group());}
    @PostMapping("/tasks/{id}/validate") public Object validate(@PathVariable("id") String id)throws Exception{return tasks.validate(id);}
    @PostMapping({"/tasks/{id}/start","/tasks/{id}/retry"}) @ResponseStatus(HttpStatus.ACCEPTED) public Object start(@PathVariable("id") String id)throws Exception{tasks.start(id);return Map.of("state","STARTING");}
    @PostMapping("/tasks/{id}/pause") @ResponseStatus(HttpStatus.ACCEPTED) public Object pause(@PathVariable("id") String id)throws Exception{tasks.stop(id,true);return Map.of("state","PAUSING");}
    @PostMapping("/tasks/{id}/stop") @ResponseStatus(HttpStatus.ACCEPTED) public Object stop(@PathVariable("id") String id)throws Exception{tasks.stop(id,false);return Map.of("state","STOPPING");}
    @PostMapping("/tasks/{id}/compare") public Object compare(@PathVariable("id") String id,@RequestBody ComparisonInput input)throws Exception{return tasks.compare(id,input.sourceQuiescent(),input.maxRows()==0?10000:input.maxRows());}
    @PutMapping("/tasks/{id}/limits") public Object limits(@PathVariable("id") String id,@RequestBody LimitsInput input)throws Exception{tasks.limits(id,input.journalBytes(),input.journalEvents(),input.batchSize(),input.maxFailures());return task(id);}
    @GetMapping("/tasks/{id}/ddl") public Object ddl(@PathVariable("id") String id)throws Exception{
        var spec=tasks.specification(id);var inspection=registry.get(spec.source().type()).inspect(spec.source(),ConnectorProvider.Role.SOURCE);
        return DdlPreview.preview(spec,inspection);
    }
    @GetMapping("/audit") public Object audit()throws Exception{return store.audits();}
}
