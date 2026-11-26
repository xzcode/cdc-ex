package com.cdcex.debezium;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Recognizes only this connector's internal heartbeat; other metadata must be handled explicitly. */
public final class DebeziumMetadata {
    private DebeziumMetadata() {}
    public static boolean isHeartbeat(SourceRecord record,String namespace) {
        if(!("__debezium-heartbeat."+namespace).equals(record.topic()))return false;
        if(record.sourcePartition()==null||!namespace.equals(record.sourcePartition().get("server")))return false;
        if(record.sourceOffset()==null||record.sourceOffset().isEmpty())return false;
        if(!(record.key() instanceof Struct key)||!(record.value() instanceof Struct value))return false;
        if(!"io.debezium.connector.common.ServerNameKey".equals(key.schema().name())
                ||!"io.debezium.connector.common.Heartbeat".equals(value.schema().name()))return false;
        if(key.schema().fields().size()!=1||key.schema().field("serverName")==null
                ||key.schema().field("serverName").schema().type()!=Schema.Type.STRING)return false;
        if(value.schema().fields().size()!=1||value.schema().field("ts_ms")==null
                ||value.schema().field("ts_ms").schema().type()!=Schema.Type.INT64)return false;
        return namespace.equals(key.getWithoutDefault("serverName"))&&value.getWithoutDefault("ts_ms") instanceof Long;
    }
}
