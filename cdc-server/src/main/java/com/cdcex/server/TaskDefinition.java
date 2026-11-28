package com.cdcex.server;

import com.cdcex.runtime.*;
import com.cdcex.core.*;
import com.cdcex.storage.ControlStore;
import java.util.*;

/** Group and resource selectors are management metadata; the resolved task spec is the generation contract. */
public record TaskDefinition(String id,String name,String sourceConnectionId,String sinkConnectionId,String snapshotMode,
                             MappingSpec mapping,long journalBytes,long journalEvents,int batchSize,int maxFailures,
                             String group,Map<String,String> sourceOverrides,Map<String,String> sinkOverrides){
    public TaskDefinition{
        ControlStore.id(id);ControlStore.id(sourceConnectionId);ControlStore.id(sinkConnectionId);
        if(name==null||name.isBlank()||name.length()>128)throw new IllegalArgumentException("Invalid task name");
        mapping=mapping==null?MappingSpec.identity():mapping;snapshotMode=snapshotMode==null?"initial":snapshotMode;
        group=normalizeGroup(group);
        sourceOverrides=sourceOverrides==null?Map.of():Map.copyOf(sourceOverrides);
        sinkOverrides=sinkOverrides==null?Map.of():Map.copyOf(sinkOverrides);
    }
    public TaskDefinition(String id,String name,String sourceConnectionId,String sinkConnectionId,String snapshotMode,
                          MappingSpec mapping,long journalBytes,long journalEvents,int batchSize,int maxFailures){
        this(id,name,sourceConnectionId,sinkConnectionId,snapshotMode,mapping,journalBytes,journalEvents,batchSize,maxFailures,"",Map.of(),Map.of());
    }
    static String normalizeGroup(String group){
        String value=group==null?"":group.strip();
        if(value.length()>80||value.codePoints().anyMatch(Character::isISOControl))throw new IllegalArgumentException("Invalid task group");
        return value;
    }
    TaskDefinition withGroup(String value){return new TaskDefinition(id,name,sourceConnectionId,sinkConnectionId,snapshotMode,mapping,journalBytes,journalEvents,batchSize,maxFailures,value,sourceOverrides,sinkOverrides);}
    TaskDefinition withLimits(long bytes,long events,int batch,int failures){return new TaskDefinition(id,name,sourceConnectionId,sinkConnectionId,snapshotMode,mapping,bytes,events,batch,failures,group,sourceOverrides,sinkOverrides);}
    static String resource(Endpoint endpoint){return endpoint.option(endpoint.type().equals("KAFKA")?"topic":"table","");}
    static Endpoint select(Endpoint template,Map<String,String> overrides,boolean source){
        if(overrides.isEmpty())return template;
        String type=template.type(),resource=type.equals("KAFKA")?"topic":"table";
        Set<String> allowed=new HashSet<>(Set.of(resource));
        if(source)switch(type){
            case "MYSQL","MARIADB"->allowed.add("replicationServerId");
            case "POSTGRESQL"->allowed.addAll(Set.of("slot","publication"));
            case "KAFKA"->allowed.add("groupId");
            default->{}
        }
        if(!allowed.containsAll(overrides.keySet())||!overrides.containsKey(resource))throw new IllegalArgumentException("Only explicit task resource selectors are permitted");
        String value=overrides.get(resource);
        String pattern=type.equals("KAFKA")?"[A-Za-z0-9._-]{1,249}":Set.of("MYSQL","MARIADB").contains(type)?"[A-Za-z0-9_]{1,64}":"[A-Za-z_][A-Za-z0-9_]{0,62}";
        if(!value.matches(pattern)||Set.of(".","..").contains(value))throw new IllegalArgumentException("Invalid task resource name");
        if(source)switch(type){
            case "MYSQL","MARIADB"->{
                String client=overrides.getOrDefault("replicationServerId","");
                if(!client.matches("[0-9]{1,10}"))throw new IllegalArgumentException("An explicit replication client ID is required per resource");
                long id=Long.parseLong(client);if(id<1||id>4294967295L)throw new IllegalArgumentException("Invalid replication client ID");
            }
            case "POSTGRESQL"->{
                if(!overrides.getOrDefault("slot","").matches("[a-z0-9_]{1,63}")||!overrides.getOrDefault("publication","").matches("[A-Za-z_][A-Za-z0-9_]{0,62}"))throw new IllegalArgumentException("Explicit pre-created slot and publication are required per resource");
            }
            case "KAFKA"->{if(!overrides.getOrDefault("groupId","").matches("[A-Za-z0-9._-]{1,180}"))throw new IllegalArgumentException("Explicit consumer group is required per resource");}
            default->{}
        }
        var options=new LinkedHashMap<>(template.options());options.putAll(overrides);return new Endpoint(type,options);
    }
    TaskSpec resolve(Endpoint source,Endpoint sink){return new TaskSpec(id,select(source,sourceOverrides,true),select(sink,sinkOverrides,false),snapshotMode,mapping,journalBytes,journalEvents,batchSize,maxFailures);}
    @Override public String toString(){return "TaskDefinition[id="+id+", configuration=redacted]";}
}
