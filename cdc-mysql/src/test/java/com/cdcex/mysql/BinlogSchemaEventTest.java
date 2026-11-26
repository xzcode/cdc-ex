package com.cdcex.mysql;

import org.junit.jupiter.api.Test;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BinlogSchemaEventTest {
    private SourceRecord event(String snapshot){
        Schema source=SchemaBuilder.struct().field("snapshot",Schema.STRING_SCHEMA).build();
        Schema envelope=SchemaBuilder.struct().field("source",source).field("ddl",Schema.STRING_SCHEMA).build();
        Struct value=new Struct(envelope).put("source",new Struct(source).put("snapshot",snapshot)).put("ddl","CREATE TABLE fixture(id INT)");
        return new SourceRecord(Map.of("server","fixture"),Map.of("pos",1),"fixture",null,envelope,value);
    }
    @Test void initialSchemaUsesSourceSnapshotMarkerEvenWhenOffsetHasNoFlag(){assertNull(BinlogSource.normalize(event("true")));assertNull(BinlogSource.normalize(event("last")));}
    @Test void liveDdlBlocksInsteadOfDiscardingIt(){assertThrows(IllegalStateException.class,()->BinlogSource.normalize(event("false")));}
    @Test void destinationDdlInSameServerDoesNotBlockAnUnrelatedSourceTable(){
        Schema change=SchemaBuilder.struct().field("id",Schema.STRING_SCHEMA).build();
        Schema envelope=SchemaBuilder.struct().field("databaseName",Schema.STRING_SCHEMA).field("tableChanges",SchemaBuilder.array(change).build()).build();
        Struct value=new Struct(envelope).put("databaseName","dst").put("tableChanges",List.of(new Struct(change).put("id","\"dst\".\"items\",\"dst\".\"items_hold\"")));
        var record=new SourceRecord(Map.of("server","fixture"),Map.of("pos",1),"fixture",null,envelope,value);
        assertNull(BinlogSource.normalize(record,"src","items"));
        assertThrows(IllegalStateException.class,()->BinlogSource.normalize(record,"dst","items"));
    }
}
