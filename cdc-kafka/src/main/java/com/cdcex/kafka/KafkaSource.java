package com.cdcex.kafka;

import com.cdcex.core.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class KafkaSource implements SourceConnector {
    private final KafkaSettings settings;
    private final ConnectorProvider.SourceContext context;
    private final AtomicBoolean stopping=new AtomicBoolean();
    private volatile KafkaConsumer<byte[],byte[]> active;
    KafkaSource(KafkaSettings settings,ConnectorProvider.SourceContext context){this.settings=settings;this.context=context;}
    @Override public void run(DurableReceiver receiver) throws Exception {
        if(stopping.get())return;
        try(Admin admin=Admin.create(settings.commonProperties(context.taskId()+"-source-admin"));
            KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(consumerProperties())) {
            active=consumer;
            KafkaMetadata initial=KafkaMetadata.read(admin,settings.topic());
            KafkaCheckpoint checkpoint=new KafkaCheckpoint(context.directory().resolve("kafka-source.json"),initial,settings.groupId(),settings.startOffset(),context.fresh());
            List<TopicPartition> initialPartitions=partitions(initial);
            Map<TopicPartition,Long> beginning=consumer.beginningOffsets(initialPartitions,Duration.ofSeconds(15));
            Map<TopicPartition,Long> end=consumer.endOffsets(initialPartitions,Duration.ofSeconds(15));
            if(context.fresh()) {
                try{
                    var group=admin.describeConsumerGroups(List.of(settings.groupId())).all().get(15,TimeUnit.SECONDS).get(settings.groupId());
                    if(group!=null&&!group.members().isEmpty())throw new IllegalStateException("A new task cannot take over an active Kafka consumer group");
                }catch(java.util.concurrent.ExecutionException e){if(!(e.getCause() instanceof GroupIdNotFoundException))throw e;}
                var previousGroup=admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata().get(15,TimeUnit.SECONDS);
                if(!previousGroup.isEmpty())throw new IllegalStateException("A new task requires a dedicated unused Kafka consumer group");
            }
            Map<Integer,Long> initialOffsets=new TreeMap<>(checkpoint.state().nextOffsets());
            for(TopicPartition partition:initialPartitions) {
                Long offset=initialOffsets.get(partition.partition());
                if(offset==null) {
                    // Only initial topic partitions honor latest. Newly added partitions always start at earliest.
                    offset=context.fresh()&&settings.startOffset().equals("latest")?end.get(partition):beginning.get(partition);
                    initialOffsets.put(partition.partition(),offset);
                }
                KafkaCheckpoint.requireRetained(offset,beginning.get(partition),end.get(partition));
            }
            checkpoint.save(initial,initialOffsets);
            consumer.subscribe(List.of(settings.topic()),new ConsumerRebalanceListener(){
                @Override public void onPartitionsRevoked(Collection<TopicPartition> revoked){commitOwned(consumer,checkpoint,revoked);}
                @Override public void onPartitionsLost(Collection<TopicPartition> lost){/* Local durable offsets remain authoritative. */}
                @Override public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
                    try {
                        KafkaMetadata metadata=KafkaMetadata.read(admin,settings.topic());
                        checkpoint.state().metadata().requireSameTopic(metadata);
                        if(assigned.size()!=metadata.partitions())throw new IllegalStateException("Kafka task requires exclusive ownership of its dedicated consumer group");
                        Map<TopicPartition,Long> start=consumer.beginningOffsets(assigned,Duration.ofSeconds(15));
                        Map<TopicPartition,Long> finish=consumer.endOffsets(assigned,Duration.ofSeconds(15));
                        Map<Integer,Long> next=new TreeMap<>(checkpoint.state().nextOffsets());
                        for(TopicPartition p:assigned) {
                            long offset=next.getOrDefault(p.partition(),start.get(p));
                            KafkaCheckpoint.requireRetained(offset,start.get(p),finish.get(p));
                            next.put(p.partition(),offset);
                        }
                        checkpoint.save(metadata,next);
                        for(TopicPartition p:assigned)consumer.seek(p,next.get(p.partition()));
                    }catch(RuntimeException e){throw e;}catch(Exception e){throw new IllegalStateException("Kafka assignment validation failed; offsets retained");}
                }
            });
            long nextIdentityCheck=0;
            while(!stopping.get()) {
                if(System.nanoTime()>=nextIdentityCheck){
                    checkpoint.state().metadata().requireSameTopic(KafkaMetadata.read(admin,settings.topic()));
                    nextIdentityCheck=System.nanoTime()+Duration.ofSeconds(5).toNanos();
                }
                ConsumerRecords<byte[],byte[]> records=consumer.poll(Duration.ofMillis(250));
                if(records.isEmpty())continue;
                checkpoint.state().metadata().requireSameTopic(KafkaMetadata.read(admin,settings.topic()));
                List<ChangeEvent> batch=new ArrayList<>();
                Map<Integer,Long> next=new TreeMap<>();
                // Each partition is iterated in broker offset order; there is no cross-partition total order.
                var partitions=new ArrayList<>(records.partitions());partitions.sort(Comparator.comparingInt(TopicPartition::partition));
                for(TopicPartition partition:partitions)for(ConsumerRecord<byte[],byte[]> record:records.records(partition)){
                    try{batch.add(KafkaEnvelope.capture(checkpoint.state().metadata(),record,settings.maxMessageBytes()));}
                    catch(IllegalArgumentException invalid){
                        Map<String,Object> blocked=new LinkedHashMap<>();
                        blocked.put("clusterId",checkpoint.state().metadata().clusterId());blocked.put("topicId",checkpoint.state().metadata().topicId());
                        blocked.put("topic",record.topic());blocked.put("partition",record.partition());blocked.put("offset",record.offset());
                        blocked.put("reason","Invalid CDC_EX envelope, entity key or message size");
                        blocked.put("payloadSha256",record.value()==null?null:HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(record.value())));
                        KafkaCheckpoint.writeJson(context.directory().resolve("kafka-blocked.json"),blocked);
                        throw invalid;
                    }
                    next.put(partition.partition(),Math.addExact(record.offset(),1));
                }
                receiver.accept(batch);             // Must commit the complete batch to the local durable journal.
                checkpoint.save(checkpoint.state().metadata(),next); // fsync + atomic rename + directory fsync.
                commitOwned(consumer,checkpoint,records.partitions()); // Broker receives NEXT offsets only afterwards.
            }
        }catch(WakeupException e){if(!stopping.get())throw e;}
        finally{active=null;}
    }
    private Properties consumerProperties(){
        Properties p=settings.consumerProperties(context.taskId()+"-source");
        p.setProperty("group.instance.id","cdc-ex-"+EventCodec.digest(List.of(context.taskId(),settings.groupId())).substring(0,24));
        return p;
    }
    private static List<TopicPartition> partitions(KafkaMetadata metadata){
        List<TopicPartition> result=new ArrayList<>();for(int i=0;i<metadata.partitions();i++)result.add(new TopicPartition(metadata.topic(),i));return result;
    }
    private static void commitOwned(KafkaConsumer<byte[],byte[]> consumer,KafkaCheckpoint checkpoint,Collection<TopicPartition> partitions){
        Map<TopicPartition,OffsetAndMetadata> offsets=new HashMap<>();
        for(TopicPartition partition:partitions){Long offset=checkpoint.state().nextOffsets().get(partition.partition());if(offset!=null)offsets.put(partition,new OffsetAndMetadata(offset));}
        if(!offsets.isEmpty())consumer.commitSync(offsets,Duration.ofSeconds(15));
    }
    @Override public void close(){stopping.set(true);KafkaConsumer<byte[],byte[]> c=active;if(c!=null)c.wakeup();}
}
