package com.cdcex.kafka;

import com.cdcex.core.*;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** CDC_EX version 1, deliberately separate from raw, plain JSON and Debezium JSON formats. */
public final class KafkaEnvelope {
    private record Frame(int schemaVersion,ChangeEvent event){}
    private static final ObjectMapper JSON=new ObjectMapper()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private KafkaEnvelope(){}
    public static byte[] encode(ChangeEvent event) {
        validate(event);
        return EventCodec.json(Map.of("schemaVersion",1,"event",event)).getBytes(StandardCharsets.UTF_8);
    }
    public static byte[] key(ChangeEvent event) {
        validate(event);
        return EventCodec.canonicalJson(List.of(event.database(),event.table(),event.key())).getBytes(StandardCharsets.UTF_8);
    }
    public static ChangeEvent decode(byte[] value) {
        if(value==null)throw new IllegalArgumentException("Kafka tombstones are not CDC_EX envelopes; use a DELETE event");
        try {
            JsonNode node=JSON.readTree(value);
            if(node==null||!node.isObject()||node.size()!=2||!node.has("schemaVersion")||!node.get("schemaVersion").isIntegralNumber()
                ||node.get("schemaVersion").intValue()!=1||!node.has("event")||!node.get("event").isObject())
                throw new IllegalArgumentException("Unsupported Kafka envelope version or structure");
            // Bind from the original bytes, not JsonNode: tree conversion can strip decimal scale.
            ChangeEvent event=JSON.readValue(value,Frame.class).event();
            validate(event);return event;
        }catch(Exception invalid){throw new IllegalArgumentException("Invalid CDC_EX Kafka envelope");}
    }
    static ChangeEvent capture(KafkaMetadata metadata,ConsumerRecord<byte[],byte[]> record,int maxBytes) {
        if(record.value()!=null&&record.value().length>maxBytes)throw new IllegalArgumentException("Kafka message exceeds maxMessageBytes");
        ChangeEvent original=decode(record.value());
        if(!Arrays.equals(key(original),record.key()))throw new IllegalArgumentException("Kafka record key does not match envelope entity key");
        Map<String,Object> position=new LinkedHashMap<>();
        position.put("kind","kafka");position.put("clusterId",metadata.clusterId());position.put("topicId",metadata.topicId());
        position.put("topic",record.topic());position.put("partition",record.partition());position.put("offset",record.offset());
        position.put("timestamp",record.timestamp());position.put("originalEventId",original.id());position.put("originalPosition",original.position());
        position.put("recordKey",Base64.getEncoder().encodeToString(record.key()));
        var headers=new ArrayList<Map<String,Object>>();
        record.headers().forEach(h->{Map<String,Object> v=new LinkedHashMap<>();v.put("key",h.key());v.put("value",h.value()==null?null:Base64.getEncoder().encodeToString(h.value()));headers.add(v);});
        position.put("headers",headers);
        String id=EventCodec.digest(List.of("kafka",metadata.clusterId(),metadata.topicId(),record.partition(),record.offset()));
        return new ChangeEvent(id,original.database(),original.table(),original.operation(),EventCodec.json(position),original.key(),original.before(),original.after(),original.sourceTimeMillis());
    }
    public static List<ChangeEvent> splitKeyChange(ChangeEvent event) {
        validate(event);
        if(event.operation()!=ChangeEvent.Operation.UPDATE||event.before()==null)return List.of(event);
        Map<String,Object> old=new LinkedHashMap<>();
        for(String name:event.key().keySet()){
            if(!event.before().containsKey(name)||event.before().get(name)==null)throw new IllegalArgumentException("Update before-image does not contain the old key");
            old.put(name,event.before().get(name));
        }
        if(EventCodec.canonicalJson(old).equals(EventCodec.canonicalJson(event.key())))return List.of(event);
        ChangeEvent delete=new ChangeEvent(EventCodec.digest(List.of(event.id(),"old-key-delete")),event.database(),event.table(),ChangeEvent.Operation.DELETE,event.position(),old,event.before(),null,event.sourceTimeMillis());
        ChangeEvent insert=new ChangeEvent(EventCodec.digest(List.of(event.id(),"new-key-insert")),event.database(),event.table(),ChangeEvent.Operation.INSERT,event.position(),event.key(),null,event.after(),event.sourceTimeMillis());
        return List.of(delete,insert);
    }
    static void validate(ChangeEvent event) {
        if(event==null||event.key()==null||event.key().isEmpty()||event.key().values().stream().anyMatch(Objects::isNull))
            throw new IllegalArgumentException("Kafka state events require a stable non-null key");
        if(!Set.of(ChangeEvent.Operation.SNAPSHOT,ChangeEvent.Operation.INSERT,ChangeEvent.Operation.UPDATE,ChangeEvent.Operation.DELETE).contains(event.operation()))
            throw new IllegalArgumentException("Kafka envelope operation is not supported");
        if(event.operation()!=ChangeEvent.Operation.DELETE){
            if(event.after()==null)throw new IllegalArgumentException("Kafka state events require a full after-image");
            for(var key:event.key().entrySet())if(!event.after().containsKey(key.getKey())||!EventCodec.json(key.getValue()).equals(EventCodec.json(event.after().get(key.getKey()))))
                throw new IllegalArgumentException("Kafka envelope key does not match after-image");
        }
    }
}
