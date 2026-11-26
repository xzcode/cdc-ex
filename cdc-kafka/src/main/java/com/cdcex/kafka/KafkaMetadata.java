package com.cdcex.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import java.util.*;
import java.util.concurrent.TimeUnit;

record KafkaMetadata(String clusterId,String topicId,String topic,int partitions) {
    static KafkaMetadata read(Admin admin,String topic) throws Exception {
        String cluster=admin.describeCluster().clusterId().get(15,TimeUnit.SECONDS);
        TopicDescription description=admin.describeTopics(List.of(topic)).allTopicNames().get(15,TimeUnit.SECONDS).get(topic);
        if(cluster==null || cluster.isBlank() || description==null || description.topicId().equals(Uuid.ZERO_UUID))
            throw new IllegalStateException("Broker did not provide a stable cluster/topic identity");
        if(description.isInternal() || description.partitions().isEmpty())
            throw new IllegalStateException("Kafka internal or empty topics are not supported");
        return new KafkaMetadata(cluster,description.topicId().toString(),topic,description.partitions().size());
    }
    String identity(){return "kafka:"+clusterId+":"+topicId+":"+topic;}
    static void requireAutoCreationDisabled(Admin admin) throws Exception {
        var brokers=admin.describeCluster().nodes().get(15,TimeUnit.SECONDS);
        var resources=brokers.stream().map(n->new ConfigResource(ConfigResource.Type.BROKER,Integer.toString(n.id()))).toList();
        var configs=admin.describeConfigs(resources).all().get(15,TimeUnit.SECONDS);
        for(var config:configs.values()){
            var setting=config.get("auto.create.topics.enable");
            if(setting==null||!"false".equalsIgnoreCase(setting.value()))
                throw new IllegalStateException("Kafka sink requires auto.create.topics.enable=false to prevent implicit topic creation");
        }
    }
    void requireSameTopic(KafkaMetadata other){
        if(!identity().equals(other.identity()))throw new IllegalStateException("Kafka cluster or topic was replaced; checkpoint reset is forbidden");
        if(other.partitions<partitions)throw new IllegalStateException("Kafka partition count decreased");
    }
    void requireSameSink(KafkaMetadata other){
        requireSameTopic(other);
        if(partitions!=other.partitions)throw new IllegalStateException("Kafka sink partition count changed; keyed ordering requires a new reviewed task generation");
    }
}
