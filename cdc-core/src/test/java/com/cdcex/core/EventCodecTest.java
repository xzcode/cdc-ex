package com.cdcex.core;

import org.junit.jupiter.api.Test;
import java.math.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EventCodecTest {
    @Test void bsonDocumentOrderAndExactNumbersSurviveJournalEncoding(){
        Map<String,Object> nested=new LinkedHashMap<>();nested.put("z",new BigInteger("9223372036854775808"));nested.put("a",new BigDecimal("12345678901234567890.1234"));
        var event=new ChangeEvent("id","db","table",ChangeEvent.Operation.INSERT,"opaque",Map.of("_id","a"),null,Map.of("document",nested),0);
        ChangeEvent decoded=EventCodec.decode(EventCodec.json(event));
        assertEquals(List.of("z","a"),new ArrayList<>(((Map<?,?>)decoded.after().get("document")).keySet()));
        assertEquals(EventCodec.json(event),EventCodec.json(decoded));
        nested.put("late",true);assertFalse(((Map<?,?>)event.after().get("document")).containsKey("late"));
    }
    @Test void payloadIdentityDoesNotEraseBsonFieldOrdering(){
        Map<String,Object> a=new LinkedHashMap<>(),b=new LinkedHashMap<>();a.put("z",1);a.put("a",2);b.put("a",2);b.put("z",1);
        assertEquals(EventCodec.digest(a),EventCodec.digest(b));
        assertNotEquals(EventCodec.payloadDigest(a),EventCodec.payloadDigest(b));
    }
}
