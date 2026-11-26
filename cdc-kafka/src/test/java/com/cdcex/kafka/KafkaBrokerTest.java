package com.cdcex.kafka;

import com.cdcex.core.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Runs only against an explicitly supplied disposable broker; every topic/group is uniquely owned. */
@EnabledIfEnvironmentVariable(named="CDC_TEST_KAFKA_BOOTSTRAP",matches=".+")
@Timeout(180)
class KafkaBrokerTest {
    @TempDir Path directory;
    private final List<String> topics=new ArrayList<>(),groups=new ArrayList<>();
    private final List<SourceConnector> sources=new ArrayList<>();
    private final ExecutorService executor=Executors.newCachedThreadPool();
    private Admin admin;
    @BeforeEach void connect(){
        var properties=new Properties();properties.setProperty("bootstrap.servers",System.getenv("CDC_TEST_KAFKA_BOOTSTRAP"));
        properties.setProperty("request.timeout.ms","10000");properties.setProperty("default.api.timeout.ms","15000");admin=Admin.create(properties);
    }
    @AfterEach void cleanup() throws Exception {
        for(var source:sources)source.close();executor.shutdownNow();executor.awaitTermination(15,TimeUnit.SECONDS);
        if(admin==null)return;
        try{if(!groups.isEmpty())admin.deleteConsumerGroups(groups).all().get(15,TimeUnit.SECONDS);}catch(Exception ignored){}
        try{if(!topics.isEmpty())admin.deleteTopics(topics).all().get(15,TimeUnit.SECONDS);}finally{admin.close(Duration.ofSeconds(5));}
    }
    private Endpoint endpoint(String topic,String group){
        return new Endpoint("KAFKA",Map.of("bootstrapServers",System.getenv("CDC_TEST_KAFKA_BOOTSTRAP"),"topic",topic,"groupId",group,
            "securityProtocol","PLAINTEXT","allowInsecure","true","startOffset","earliest"));
    }
    private String topic(int partitions) throws Exception {
        String topic="cdcex-test-"+UUID.randomUUID();topics.add(topic);
        admin.createTopics(List.of(new NewTopic(topic,partitions,(short)1))).all().get(15,TimeUnit.SECONDS);return topic;
    }
    private String group(){String group="cdcex-group-"+UUID.randomUUID();groups.add(group);return group;}
    private SourceConnector source(Endpoint endpoint,String task,boolean fresh){
        var source=new KafkaProvider().openSource(endpoint,new ConnectorProvider.SourceContext(directory.resolve(task),task,"initial",fresh));
        sources.add(source);return source;
    }
    private Future<?> run(SourceConnector source,SourceConnector.DurableReceiver receiver){return executor.submit(()->{source.run(receiver);return null;});}
    private ChangeEvent event(int key,String value){return new ChangeEvent("id-"+key+"-"+value,"source","items",ChangeEvent.Operation.INSERT,"p-"+key,Map.of("id",key),null,Map.of("id",key,"name",value),1);}
    private void send(Endpoint endpoint,int partition,ChangeEvent event) throws Exception {
        var settings=new KafkaSettings(endpoint);
        try(var producer=new KafkaProducer<byte[],byte[]>(settings.producerProperties("test"))){
            producer.send(new ProducerRecord<>(settings.topic(),partition,KafkaEnvelope.key(event),KafkaEnvelope.encode(event))).get(35,TimeUnit.SECONDS);
        }
    }
    private static void await(BooleanSupplier test) throws Exception {
        long deadline=System.nanoTime()+Duration.ofSeconds(40).toNanos();
        while(!test.getAsBoolean()){if(System.nanoTime()>deadline)fail("Kafka integration condition timed out");Thread.sleep(100);}
    }
    @Test void deliveryRestartAndPartitionAddition() throws Exception {
        String topic=topic(2),group=group();Endpoint endpoint=endpoint(topic,group);
        var seen=new CopyOnWriteArrayList<ChangeEvent>();var first=source(endpoint,"reader",true);Future<?> firstFuture=run(first,seen::addAll);
        try(var sink=new KafkaProvider().openSink(endpoint,new ConnectorProvider.SinkContext(directory.resolve("writer"),"writer"))){
            sink.apply(List.of(event(1,"a"),event(2,"b")));await(()->seen.size()==2);
        }
        first.close();firstFuture.get(20,TimeUnit.SECONDS);
        // Reopening a sink validates the persisted topic identity and partition count.
        try(var sink=new KafkaProvider().openSink(endpoint,new ConnectorProvider.SinkContext(directory.resolve("writer"),"writer",false))){sink.apply(List.of(event(3,"c")));}
        var resumed=source(endpoint,"reader",false);Future<?> resumedFuture=run(resumed,seen::addAll);await(()->seen.size()==3);
        admin.createPartitions(Map.of(topic,NewPartitions.increaseTo(3))).all().get(15,TimeUnit.SECONDS);
        send(endpoint,2,event(4,"new-partition"));await(()->seen.size()==4);
        assertTrue(seen.get(3).position().contains("\"partition\":2"));
        assertThrows(IllegalStateException.class,()->new KafkaProvider().openSink(endpoint,new ConnectorProvider.SinkContext(directory.resolve("writer"),"writer",false)));
        resumed.close();resumedFuture.get(20,TimeUnit.SECONDS);
        assertEquals(4,seen.stream().map(ChangeEvent::id).distinct().count());
    }
    @Test void failedDurableReceiverLeavesOffsetsForReplayAndExpiredOffsetsBlock() throws Exception {
        String topic=topic(1),group=group();Endpoint endpoint=endpoint(topic,group);send(endpoint,0,event(1,"first"));
        var failing=source(endpoint,"reader",true);Future<?> failure=run(failing,batch->{throw new IllegalStateException("injected journal failure");});
        assertThrows(ExecutionException.class,()->failure.get(40,TimeUnit.SECONDS));
        var state=new KafkaCheckpoint(directory.resolve("reader/kafka-source.json"),KafkaMetadata.read(admin,topic),group,"earliest",false);
        assertEquals(0L,state.state().nextOffsets().get(0));
        var accepted=new CopyOnWriteArrayList<ChangeEvent>();var replay=source(endpoint,"reader",false);Future<?> replayFuture=run(replay,accepted::addAll);
        await(()->accepted.size()==1);replay.close();replayFuture.get(20,TimeUnit.SECONDS);
        send(endpoint,0,event(2,"expired"));
        admin.deleteRecords(Map.of(new TopicPartition(topic,0),RecordsToDelete.beforeOffset(2))).all().get(15,TimeUnit.SECONDS);
        var expired=source(endpoint,"reader",false);assertThrows(ExecutionException.class,()->run(expired,accepted::addAll).get(30,TimeUnit.SECONDS));
        assertEquals(1,accepted.size());
    }
    @Test void recreatedTopicAndInvalidPayloadNeverAdvanceCheckpoint() throws Exception {
        String topic=topic(1),group=group();Endpoint endpoint=endpoint(topic,group);
        var settings=new KafkaSettings(endpoint);
        try(var producer=new KafkaProducer<byte[],byte[]>(settings.producerProperties("invalid"))){producer.send(new ProducerRecord<>(topic,new byte[]{'k'},new byte[]{'{','}'})).get(35,TimeUnit.SECONDS);}
        var invalid=source(endpoint,"reader",true);
        assertThrows(ExecutionException.class,()->run(invalid,batch->fail("Invalid message reached receiver")).get(40,TimeUnit.SECONDS));
        var state=new KafkaCheckpoint(directory.resolve("reader/kafka-source.json"),KafkaMetadata.read(admin,topic),group,"earliest",false);
        assertEquals(0L,state.state().nextOffsets().get(0));
        admin.deleteTopics(List.of(topic)).all().get(15,TimeUnit.SECONDS);
        await(()->{try{return !admin.listTopics().names().get(5,TimeUnit.SECONDS).contains(topic);}catch(Exception e){return false;}});
        admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(15,TimeUnit.SECONDS);
        var replaced=source(endpoint,"reader",false);
        assertThrows(ExecutionException.class,()->run(replaced,batch->fail("Replacement topic reached receiver")).get(30,TimeUnit.SECONDS));
    }
    @Test void sharedConsumerGroupIsRejectedDuringRebalance() throws Exception {
        String topic=topic(2),group=group();Endpoint endpoint=endpoint(topic,group);
        var accepted=new CopyOnWriteArrayList<ChangeEvent>();var source=source(endpoint,"exclusive",true);Future<?> worker=run(source,accepted::addAll);
        send(endpoint,0,event(1,"owned"));await(()->accepted.size()==1);
        var properties=new KafkaSettings(endpoint).consumerProperties("unexpected-peer");
        try(var peer=new KafkaConsumer<byte[],byte[]>(properties)){
            peer.subscribe(List.of(topic));
            long deadline=System.nanoTime()+Duration.ofSeconds(40).toNanos();
            while(!worker.isDone()&&System.nanoTime()<deadline)peer.poll(Duration.ofMillis(500));
            assertThrows(ExecutionException.class,()->worker.get(10,TimeUnit.SECONDS));
        }
    }
}
