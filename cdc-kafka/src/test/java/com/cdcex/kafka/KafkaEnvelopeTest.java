package com.cdcex.kafka;

import com.cdcex.core.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import java.math.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaEnvelopeTest {
    private static ChangeEvent event(){
        Map<String,Object> after=new LinkedHashMap<>();after.put("id",new BigInteger("9223372036854775808"));
        after.put("amount",new BigDecimal("12345678901234567890.1200"));after.put("nullable",null);
        after.put("objectId",Map.of("$oid","507f1f77bcf86cd799439011"));
        return new ChangeEvent("source-event","db","items",ChangeEvent.Operation.INSERT,"source-offset",Map.of("id",after.get("id")),null,after,1234);
    }
    @Test void envelopePreservesPrecisionNullAndBsonWrappers(){
        ChangeEvent original=event();ChangeEvent decoded=KafkaEnvelope.decode(KafkaEnvelope.encode(original));
        assertEquals(EventCodec.json(original),EventCodec.json(decoded));
        assertTrue(decoded.after().containsKey("nullable"));assertNull(decoded.after().get("nullable"));assertFalse(decoded.after().containsKey("missing"));
        assertEquals(new BigDecimal("12345678901234567890.1200"),decoded.after().get("amount"));
    }
    @Test void captureIdentityBelongsToKafkaPositionAndRetainsOrigin(){
        ChangeEvent original=event();var metadata=new KafkaMetadata("cluster","topic-id","events",2);
        var first=new ConsumerRecord<byte[],byte[]>("events",1,42,KafkaEnvelope.key(original),KafkaEnvelope.encode(original));
        first.headers().add("binary",new byte[]{0,(byte)255});first.headers().add("nullable",null);
        ChangeEvent captured=KafkaEnvelope.capture(metadata,first,100000);
        assertEquals(captured,KafkaEnvelope.capture(metadata,first,100000));
        var duplicate=new ConsumerRecord<byte[],byte[]>("events",1,43,KafkaEnvelope.key(original),KafkaEnvelope.encode(original));
        assertNotEquals(captured.id(),KafkaEnvelope.capture(metadata,duplicate,100000).id());
        assertTrue(captured.position().contains("source-event"));assertTrue(captured.position().contains("source-offset"));
        assertTrue(captured.position().contains("AP8="));
    }
    @Test void incompatibleMessagesNeverBecomeStateChanges(){
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.decode(null));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.decode("{\"schemaVersion\":2,\"event\":{}}".getBytes(StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.decode("{}{}".getBytes(StandardCharsets.UTF_8)));
        var record=new ConsumerRecord<byte[],byte[]>("events",0,1,new byte[]{'x'},KafkaEnvelope.encode(event()));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.capture(new KafkaMetadata("c","t","events",1),record,100000));
        assertThrows(IllegalArgumentException.class,()->KafkaEnvelope.capture(new KafkaMetadata("c","t","events",1),record,1));
    }
    @Test void mutableKeyIsAnOrderedDeleteAndInsertWithStableReplayIdentity(){
        var update=new ChangeEvent("up","db","items",ChangeEvent.Operation.UPDATE,"p",Map.of("id",2),Map.of("id",1,"name","old"),Map.of("id",2,"name","new"),1);
        var split=KafkaEnvelope.splitKeyChange(update);
        assertEquals(List.of(ChangeEvent.Operation.DELETE,ChangeEvent.Operation.INSERT),split.stream().map(ChangeEvent::operation).toList());
        assertEquals(Map.of("id",1),split.get(0).key());assertEquals(Map.of("id",2),split.get(1).key());
        assertEquals(split,KafkaEnvelope.splitKeyChange(update));
        assertFalse(Arrays.equals(KafkaEnvelope.key(split.get(0)),KafkaEnvelope.key(split.get(1))));
    }
}
