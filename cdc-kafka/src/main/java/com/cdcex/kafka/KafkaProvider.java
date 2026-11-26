package com.cdcex.kafka;

import com.cdcex.core.*;
import org.apache.kafka.clients.admin.Admin;
import java.util.*;

public final class KafkaProvider implements ConnectorProvider {
    @Override public String type(){return "KAFKA";}
    @Override public Map<String,Object> capabilities(){
        return Map.of("sourceModes",List.of("earliest","latest"),"startOffsets",List.of("earliest","latest"),"sinkModes",List.of("CDC_EX"),
            "operations",List.of("SNAPSHOT","INSERT","UPDATE","DELETE"),"nativeCdc",false,
            "rawMessages",false,"delivery","at-least-once","ordering","within each partition",
            "consumerGroup","one task owner, automatic rebalance","security",List.of("SSL","SASL_SSL","PLAINTEXT with explicit opt-in"));
    }
    @Override public Inspection inspect(Endpoint endpoint,Role role) throws Exception {
        KafkaSettings settings=new KafkaSettings(endpoint);
        if(role==Role.SOURCE)settings.groupId();
        try(Admin admin=Admin.create(settings.commonProperties("preflight"))){
            KafkaMetadata metadata=KafkaMetadata.read(admin,settings.topic());
            if(role==Role.SINK)KafkaMetadata.requireAutoCreationDisabled(admin);
            return new Inspection(metadata.identity(),Map.of(),List.of(),Map.of("topic",metadata.topic(),"partitions",metadata.partitions(),
                "clusterId",metadata.clusterId(),"topicId",metadata.topicId(),"format","CDC_EX/1","schema","declared in each envelope"));
        }
    }
    @Override public SourceConnector openSource(Endpoint endpoint,SourceContext context){
        if(Set.of("earliest","latest").contains(context.snapshotMode())){
            var options=new LinkedHashMap<>(endpoint.options());options.put("startOffset",context.snapshotMode());endpoint=new Endpoint("KAFKA",options);
        }else if(!"initial".equals(context.snapshotMode()))throw new IllegalArgumentException("Kafka source mode must be earliest or latest; saved offsets always resume");
        return new KafkaSource(new KafkaSettings(endpoint),context);
    }
    @Override public SinkConnector openSink(Endpoint endpoint,SinkContext context) throws Exception{return new KafkaSink(new KafkaSettings(endpoint),context);}
}
