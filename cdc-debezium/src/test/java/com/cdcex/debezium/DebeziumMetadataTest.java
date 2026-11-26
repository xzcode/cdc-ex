package com.cdcex.debezium;

import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class DebeziumMetadataTest {
    @Test void recognizesPinnedConnectorHeartbeatWithoutConfusingRowsOrOtherTasks() throws Exception {
        String namespace="cdc-ex-pg-empty";
        List<SourceRecord> records=new ArrayList<>();
        new HeartbeatImpl(Duration.ofSeconds(1),"__debezium-heartbeat."+namespace,namespace,SchemaNameAdjuster.create())
                .forcedBeat(Map.of("server",namespace),Map.of("lsn",64L,"snapshot_completed",true),records::add);
        assertEquals(1,records.size());SourceRecord record=records.getFirst();
        assertTrue(DebeziumMetadata.isHeartbeat(record,namespace));
        assertFalse(DebeziumMetadata.isHeartbeat(record,"cdc-ex-pg-other"));
        assertFalse(DebeziumMetadata.isHeartbeat(new SourceRecord(Map.of("server","other"),record.sourceOffset(),
                record.topic(),0,record.keySchema(),record.key(),record.valueSchema(),record.value()),namespace));
        assertFalse(DebeziumMetadata.isHeartbeat(new SourceRecord(record.sourcePartition(),Map.of(),
                record.topic(),0,record.keySchema(),record.key(),record.valueSchema(),record.value()),namespace));
        var rowSchema=SchemaBuilder.struct().name("app.table.Value").field("ts_ms",org.apache.kafka.connect.data.Schema.INT64_SCHEMA).build();
        assertFalse(DebeziumMetadata.isHeartbeat(new SourceRecord(record.sourcePartition(),record.sourceOffset(),
                record.topic(),0,record.keySchema(),record.key(),rowSchema,new Struct(rowSchema).put("ts_ms",1L)),namespace));
    }
}
