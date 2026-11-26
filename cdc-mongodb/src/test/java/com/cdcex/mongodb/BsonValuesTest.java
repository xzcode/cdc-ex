package com.cdcex.mongodb;

import com.cdcex.core.*;
import org.bson.*;
import org.bson.types.*;
import org.junit.jupiter.api.Test;
import java.math.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BsonValuesTest {
    @Test void canonicalBsonSurvivesJournalEncodingWithoutLosingTypesOrMissingFields() {
        BsonDocument doc=new BsonDocument("_id",new BsonObjectId(new ObjectId("507f1f77bcf86cd799439011")))
            .append("small",new BsonInt32(3)).append("large",new BsonInt64(9_007_199_254_740_993L))
            .append("decimal",new BsonDecimal128(new Decimal128(new BigDecimal("12345678901234567890.12345678901234"))))
            .append("floating",new BsonDouble(0.1)).append("bytes",new BsonBinary((byte)0,new byte[]{0,1,-1}))
            .append("when",new BsonDateTime(1_735_689_600_123L)).append("timestamp",new BsonTimestamp(123,4))
            .append("explicitNull",BsonNull.VALUE)
            .append("nested",new BsonDocument("values",new BsonArray(List.of(new BsonString("x"),new BsonInt64(2)))));
        var event=new ChangeEvent("x","db","items",ChangeEvent.Operation.INSERT,"p",BsonValues.key(doc),null,BsonValues.map(doc),1);
        ChangeEvent replay=EventCodec.decode(EventCodec.json(event));
        assertEquals(doc,BsonValues.document(replay.after()));
        assertTrue(replay.after().containsKey("explicitNull"));
        assertFalse(replay.after().containsKey("missing"));
    }
    @Test void sqlNumbersMapExactlyAndOverflowNeverRoundsSilently() {
        var bson=BsonValues.document(Map.of("_id",BigInteger.ONE,"amount",new BigDecimal("123.4500")));
        assertEquals(new BsonInt64(1),bson.get("_id"));
        assertEquals(new BigDecimal("123.4500"),bson.getDecimal128("amount").getValue().bigDecimalValue());
        assertThrows(ArithmeticException.class,()->BsonValues.document(Map.of("n",BigInteger.ONE.shiftLeft(64))));
        assertThrows(NumberFormatException.class,()->BsonValues.document(Map.of("n",new BigDecimal("12345678901234567890123456789012345"))));
    }
    @Test void dollarDocumentsCannotBeConfusedWithExtendedJsonTypeTags() {
        BsonDocument ambiguous=new BsonDocument("a",new BsonDocument("$numberLong",new BsonString("3")));
        assertThrows(IllegalArgumentException.class,()->BsonValues.map(ambiguous));
        assertThrows(IllegalArgumentException.class,()->BsonValues.key(new BsonDocument("_id",BsonNull.VALUE)));
    }
    @Test void snapshotIdentityIsStableForAnInterruptedSameTimestampReplay() {
        Endpoint endpoint=new Endpoint("MONGODB",Map.of("database","src","table","items"));
        var state=new MongoCheckpoint.State("collection-uuid","initial","snapshot",new BsonTimestamp(100,3),null);
        var row=new BsonDocument("_id",new BsonInt64(1)).append("value",new BsonString("before"));
        assertEquals(EventCodec.json(MongoSource.snapshotEvent(endpoint,state,row)),EventCodec.json(MongoSource.snapshotEvent(endpoint,state,row)));
        var later=new MongoCheckpoint.State("collection-uuid","initial","snapshot",new BsonTimestamp(101,3),null);
        assertNotEquals(MongoSource.snapshotEvent(endpoint,state,row).id(),MongoSource.snapshotEvent(endpoint,later,row).id());
    }
}
