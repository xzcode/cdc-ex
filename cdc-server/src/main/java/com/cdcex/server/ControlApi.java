package com.cdcex.server;

import com.cdcex.core.*;
import com.cdcex.storage.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api")
public final class ControlApi {
    private final ControlStore store;private final ConnectorRegistry registry;private final TaskManager tasks;
    public ControlApi(ControlStore store,ConnectorRegistry registry,TaskManager tasks){this.store=store;this.registry=registry;this.tasks=tasks;}
    public record ConnectionInput(String id,String name,Endpoint endpoint){}
    public record ComparisonInput(boolean sourceQuiescent,int maxRows){}
    @GetMapping("/health") public Map<String,String> health(){return Map.of("status","UP","product","CDC EX");}
    @GetMapping("/connectors") public Object connectors(){return registry.capabilities();}
    @GetMapping("/connections") public Object connections()throws Exception{List<Map<String,Object>> rows=new ArrayList<>();for(var r:store.list("connection"))rows.add(publicConnection(r));return rows;}
    @PostMapping("/connections") @ResponseStatus(HttpStatus.CREATED) public Object connection(@RequestBody ConnectionInput input)throws Exception{
        Objects.requireNonNull(input.endpoint());registry.get(input.endpoint().type());
        store.create("connection",input.id(),input.name(),CdcApplication.JSON.writeValueAsString(input.endpoint()));return publicConnection(store.get("connection",input.id()));
    }
    @PutMapping("/connections/{id}") public Object connection(@PathVariable String id,@RequestBody ConnectionInput input)throws Exception{
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
    @PostMapping("/connections/{id}/test") public Object test(@PathVariable String id,@RequestParam(defaultValue="SOURCE") ConnectorProvider.Role role)throws Exception{
        Endpoint endpoint=tasks.connection(id);var result=registry.get(endpoint.type()).inspect(endpoint,role);store.audit("TEST_CONNECTION",id,"OK");return result;
    }
    @GetMapping("/tasks") public Object tasks()throws Exception{return tasks.views();}
    @GetMapping("/tasks/{id}") public Object task(@PathVariable String id)throws Exception{return tasks.view(store.get("task",id));}
    @PostMapping("/tasks") @ResponseStatus(HttpStatus.CREATED) public Object task(@RequestBody TaskDefinition definition)throws Exception{
        definition.resolve(tasks.connection(definition.sourceConnectionId()),tasks.connection(definition.sinkConnectionId()));
        store.create("task",definition.id(),definition.name(),CdcApplication.JSON.writeValueAsString(definition));return task(definition.id());
    }
    @PostMapping("/tasks/{id}/validate") public Object validate(@PathVariable String id)throws Exception{return tasks.validate(id);}
    @PostMapping({"/tasks/{id}/start","/tasks/{id}/retry"}) @ResponseStatus(HttpStatus.ACCEPTED) public Object start(@PathVariable String id)throws Exception{tasks.start(id);return Map.of("state","STARTING");}
    @PostMapping("/tasks/{id}/pause") @ResponseStatus(HttpStatus.ACCEPTED) public Object pause(@PathVariable String id)throws Exception{tasks.stop(id,true);return Map.of("state","PAUSING");}
    @PostMapping("/tasks/{id}/stop") @ResponseStatus(HttpStatus.ACCEPTED) public Object stop(@PathVariable String id)throws Exception{tasks.stop(id,false);return Map.of("state","STOPPING");}
    @PostMapping("/tasks/{id}/compare") public Object compare(@PathVariable String id,@RequestBody ComparisonInput input)throws Exception{return tasks.compare(id,input.sourceQuiescent(),input.maxRows()==0?10000:input.maxRows());}
    @GetMapping("/tasks/{id}/ddl") public Object ddl(@PathVariable String id)throws Exception{
        var spec=tasks.specification(id);var inspection=registry.get(spec.source().type()).inspect(spec.source(),ConnectorProvider.Role.SOURCE);
        return DdlPreview.preview(spec,inspection);
    }
    @GetMapping("/audit") public Object audit()throws Exception{return store.audits();}
}
