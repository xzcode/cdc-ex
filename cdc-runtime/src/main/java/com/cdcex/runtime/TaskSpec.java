package com.cdcex.runtime;

import com.cdcex.core.*;
import java.util.*;

public record TaskSpec(String id, Endpoint source, Endpoint sink, String snapshotMode, MappingSpec mapping,
                       long journalBytes, long journalEvents, int batchSize, int maxFailures) {
    public TaskSpec {
        if(id==null||!id.matches("[A-Za-z0-9_-]{1,64}"))throw new IllegalArgumentException("Task ID must use letters, numbers, underscores or hyphens");
        Objects.requireNonNull(source);Objects.requireNonNull(sink);
        snapshotMode=snapshotMode==null?"initial":snapshotMode;
        if(source.type().equals("KAFKA")){
            if(snapshotMode.equals("initial"))snapshotMode=source.option("startOffset","earliest");
            if(!Set.of("earliest","latest").contains(snapshotMode))throw new IllegalArgumentException("Kafka creation mode must be earliest or latest; resume uses the saved task");
            Map<String,String> options=new LinkedHashMap<>(source.options());options.put("startOffset",snapshotMode);source=new Endpoint(source.type(),options);
        }
        if(source.type().equals("CLICKHOUSE")){
            if(snapshotMode.equals("initial"))snapshotMode=source.option("mode","snapshot");
            if(!Set.of("snapshot","cursor","changelog").contains(snapshotMode))throw new IllegalArgumentException("ClickHouse capture is snapshot, cursor or changelog only");
            Map<String,String> options=new LinkedHashMap<>(source.options());options.put("mode",snapshotMode);source=new Endpoint(source.type(),options);
        }
        mapping=mapping==null?MappingSpec.identity():mapping;
        journalBytes=journalBytes==0?268435456:journalBytes;journalEvents=journalEvents==0?1000000:journalEvents;
        batchSize=batchSize==0?128:batchSize;maxFailures=maxFailures==0?12:maxFailures;
        if(journalBytes<1024||journalBytes>1099511627776L||journalEvents<1||journalEvents>1000000000L||batchSize<1||batchSize>10000||maxFailures<1||maxFailures>1000)throw new IllegalArgumentException("Invalid task resource limits");
    }
    public String fingerprint(ConnectorProvider.Inspection a,ConnectorProvider.Inspection b){
        return EventCodec.digest(List.of(id,safe(source),safe(sink),snapshotMode,mapping,a.identity(),a.schemaFingerprint(),schemaDetails(a,false),b.identity(),b.schemaFingerprint(),schemaDetails(b,true)));
    }
    private static Object schemaDetails(ConnectorProvider.Inspection value,boolean sink){
        Map<String,Object> details=new TreeMap<>();
        for(String k:List.of("schema","schemaOptions","topicId"))if(value.details().containsKey(k))details.put(k,value.details().get(k));
        if(sink&&value.details().containsKey("partitions"))details.put("partitions",value.details().get("partitions"));
        return details;
    }
    private static Object safe(Endpoint e){
        Map<String,String> options=new TreeMap<>(e.options());
        options.keySet().removeIf(k->k.toLowerCase(Locale.ROOT).matches(".*(password|secret|token|credential).*"));
        // A URI can embed credentials; hash the whole value so it never reaches diagnostics.
        return Map.of("type",e.type(),"configurationHash",EventCodec.digest(options));
    }
    @Override public String toString(){return "TaskSpec[id="+id+", configuration=redacted]";}
}
