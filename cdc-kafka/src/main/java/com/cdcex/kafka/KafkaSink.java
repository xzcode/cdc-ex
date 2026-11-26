package com.cdcex.kafka;

import com.cdcex.core.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

final class KafkaSink implements SinkConnector {
    private final KafkaSettings settings;
    private final Admin admin;
    private final KafkaProducer<byte[],byte[]> producer;
    private final KafkaMetadata metadata;
    KafkaSink(KafkaSettings settings,ConnectorProvider.SinkContext context) throws Exception {
        this.settings=settings;
        Admin newAdmin=Admin.create(settings.commonProperties(context.taskId()+"-sink-admin"));
        KafkaProducer<byte[],byte[]> newProducer=null;
        try {
            metadata=KafkaMetadata.read(newAdmin,settings.topic());
            KafkaMetadata.requireAutoCreationDisabled(newAdmin);
            Path path=context.directory().resolve("kafka-sink.json");
            KafkaCheckpoint state=new KafkaCheckpoint(path,metadata,"sink","earliest",context.fresh());
            state.state().metadata().requireSameSink(metadata);
            if(!Files.exists(path))state.save(metadata,Map.of());
            newProducer=new KafkaProducer<>(settings.producerProperties(context.taskId()+"-sink"));
            admin=newAdmin;producer=newProducer;
        }catch(Exception e){if(newProducer!=null)newProducer.close(java.time.Duration.ofSeconds(5));newAdmin.close(java.time.Duration.ofSeconds(5));throw e;}
    }
    @Override public void apply(List<ChangeEvent> events) throws Exception {
        metadata.requireSameSink(KafkaMetadata.read(admin,settings.topic()));
        List<ProducerRecord<byte[],byte[]>> records=new ArrayList<>();
        // Validate the complete batch before sending any message.
        for(ChangeEvent event:events)for(ChangeEvent part:KafkaEnvelope.splitKeyChange(event)){
            byte[] value=KafkaEnvelope.encode(part);
            if(value.length>settings.maxMessageBytes())throw new IllegalArgumentException("Kafka envelope exceeds maxMessageBytes");
            ProducerRecord<byte[],byte[]> record=new ProducerRecord<>(settings.topic(),KafkaEnvelope.key(part),value);
            record.headers().add("cdc-ex-event-id",part.id().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            record.headers().add("cdc-ex-schema-version",new byte[]{'1'});
            records.add(record);
        }
        List<Future<RecordMetadata>> futures=new ArrayList<>();
        for(var record:records)futures.add(producer.send(record));
        for(var future:futures)future.get(35,TimeUnit.SECONDS);
    }
    @Override public void close(){producer.close(java.time.Duration.ofSeconds(10));admin.close(java.time.Duration.ofSeconds(5));}
}
