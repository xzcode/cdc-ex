package com.cdcex.clickhouse;

import com.cdcex.core.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.math.*;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseContractTest {
    @TempDir Path directory;
    private static ChangeEvent event(ChangeEvent.Operation op,Map<String,Object> key,Map<String,Object> before,Map<String,Object> after) {
        return new ChangeEvent("stable-id","dst","items",op,"source-offset",key,before,after,123);
    }
    @Test void replayKeepsVersionAndKeyChangeHasAnEarlierOldKeyTombstone() {
        var event=event(ChangeEvent.Operation.UPDATE,Map.of("id",2),Map.of("id",1,"name","old"),Map.of("id",2,"name","new"));
        var delivery=new SequencedSinkConnector.Delivery(42,event);
        var first=ClickHouseSink.encode("current_state",delivery);
        var replay=ClickHouseSink.encode("current_state",new SequencedSinkConnector.Delivery(42,EventCodec.decode(EventCodec.json(event))));
        assertEquals(EventCodec.canonicalJson(first),EventCodec.canonicalJson(replay));
        assertEquals(2,first.size());assertEquals("84",first.getFirst().get("version"));
        assertEquals(1,first.getFirst().get("is_deleted"));assertEquals("85",first.getLast().get("version"));
        assertEquals(0,first.getLast().get("is_deleted"));
    }
    @Test void exhaustedVersionRangeAndAbsentOldKeyNeverPublishAnUnsafeUpdate() {
        var e=event(ChangeEvent.Operation.UPDATE,Map.of("id",1),null,Map.of("id",1));
        assertThrows(IllegalArgumentException.class,()->ClickHouseSink.encode("current_state",new SequencedSinkConnector.Delivery(2,e)));
        var insert=event(ChangeEvent.Operation.INSERT,Map.of("id",1),null,Map.of("id",1));
        assertThrows(ArithmeticException.class,()->ClickHouseSink.encode("current_state",new SequencedSinkConnector.Delivery(Long.MAX_VALUE,insert)));
    }
    @Test void compoundCursorBindsValuesInsteadOfSplicingSql() {
        Endpoint e=new Endpoint("CLICKHOUSE",Map.of("database","src","table","items"));
        var table=new ClickHouseProvider.Table("uuid","MergeTree","MergeTree()","seq,id","",Map.of("seq","UInt64","id","String"));
        String value="x') OR 1=1 --";
        var query=ClickHouseSource.cursorQuery(e,table,List.of("seq","id"),List.of("9007199254740993",value));
        assertFalse(query.sql().contains(value));assertTrue(query.sql().contains("{cursor0:UInt64}"));
        assertEquals(value,query.parameters().get("cursor1"));assertTrue(query.sql().contains("ORDER BY tuple(`seq`,`id`)"));
    }
    @Test void decimalsAndUnsignedIntegersRemainExactAcrossWireDecoding() {
        var row=ClickHouseProvider.typed(Map.of("id","18446744073709551615","amount","1234567890123.4500"),
            new LinkedHashMap<>(Map.of("id","UInt64","amount","Decimal(20, 4)")));
        assertEquals(new BigInteger("18446744073709551615"),row.get("id"));
        assertEquals(new BigDecimal("1234567890123.4500"),row.get("amount"));
    }
    @Test void durableCursorRetainsSourceGenerationAndCompoundBoundary()throws Exception {
        var checkpoint=new ClickHouseCheckpoint(directory);
        var state=new ClickHouseCheckpoint.State(1,"uuid","schema","cursor","stream","generation",List.of("9","z"));
        checkpoint.write(state);assertEquals(state,new ClickHouseCheckpoint(directory).read());
    }
    @Test void invalidChangelogOperationAndMissingImagesAreBlocked() {
        Endpoint e=new Endpoint("CLICKHOUSE",Map.of("database","src","table","changes"));
        Map<String,Object> row=new HashMap<>();row.put("sequence",BigInteger.ONE);row.put("event_id","one");
        row.put("operation","TRUNCATE");row.put("key_json","{\"id\":1}");row.put("source_time_ms",BigInteger.ONE);
        assertThrows(IllegalArgumentException.class,()->ClickHouseSource.changelog(e,"uuid",row));
        row.put("operation","UPDATE");row.put("after_json","{\"id\":1}");
        assertThrows(IllegalArgumentException.class,()->ClickHouseSource.changelog(e,"uuid",row));
    }
}
