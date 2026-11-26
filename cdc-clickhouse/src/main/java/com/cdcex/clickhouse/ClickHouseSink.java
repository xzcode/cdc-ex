package com.cdcex.clickhouse;

import com.cdcex.core.*;
import java.util.*;

final class ClickHouseSink implements SequencedSinkConnector {
    private final Endpoint endpoint;
    private final ClickHouseHttp http;
    private final String fingerprint,mode;
    ClickHouseSink(Endpoint endpoint,ConnectorProvider.SinkContext context)throws Exception {
        this.endpoint=endpoint;mode=ClickHouseProvider.sinkMode(endpoint);
        ClickHouseHttp opened=new ClickHouseHttp(endpoint);
        try {
            var table=ClickHouseProvider.table(opened,endpoint);ClickHouseProvider.validateSink(endpoint,table);
            fingerprint=table.fingerprint();
            if(mode.equals("current_state")&&context.fresh()
                &&!opened.query("SELECT 1 AS present FROM "+ClickHouseProvider.qualified(endpoint)+" LIMIT 1",Map.of()).isEmpty())
                throw new IllegalStateException("A new versioned task requires an empty ClickHouse target");
            http=opened;
        } catch(Exception e){opened.close();throw e;}
    }
    @Override public void applySequenced(List<Delivery> events)throws Exception {
        if(!fingerprint.equals(ClickHouseProvider.table(http,endpoint).fingerprint()))
            throw new IllegalStateException("ClickHouse target schema or identity changed");
        StringBuilder body=new StringBuilder();
        for(Delivery delivery:events) {
            ChangeEvent event=delivery.event();
            if(!event.database().equals(endpoint.require("database"))||!event.table().equals(endpoint.require("table")))
                throw new IllegalArgumentException("ClickHouse event namespace is not mapped to this target");
            for(var row:encode(mode,delivery))body.append(EventCodec.json(row)).append('\n');
        }
        if(!body.isEmpty())http.insert("INSERT INTO "+ClickHouseProvider.qualified(endpoint),body.toString());
    }
    static List<Map<String,Object>> encode(String mode,Delivery delivery) {
        ChangeEvent e=delivery.event();
        if(!Set.of(ChangeEvent.Operation.SNAPSHOT,ChangeEvent.Operation.INSERT,ChangeEvent.Operation.UPDATE,ChangeEvent.Operation.DELETE).contains(e.operation()))
            throw new IllegalArgumentException("Unsupported ClickHouse sink operation");
        if(e.key()==null || e.key().isEmpty() || e.key().values().stream().anyMatch(Objects::isNull))
            throw new IllegalArgumentException("ClickHouse requires a non-null stable entity key");
        if(e.operation()!=ChangeEvent.Operation.DELETE && e.after()==null)throw new IllegalArgumentException("ClickHouse event is missing its post-image");
        if(mode.equals("history")) {
            Map<String,Object> row=new LinkedHashMap<>();
            row.put("event_id",e.id());row.put("entity_key",EventCodec.canonicalJson(e.key()));row.put("operation",e.operation().name());
            row.put("before_json",e.before()==null?null:EventCodec.json(e.before()));row.put("after_json",e.after()==null?null:EventCodec.json(e.after()));
            row.put("source_position",e.position());row.put("source_time_ms",e.sourceTimeMillis());
            return List.of(row);
        }
        if(delivery.sequence()<1)throw new IllegalArgumentException("ClickHouse versions require positive durable journal sequences");
        long version=Math.addExact(Math.multiplyExact(delivery.sequence(),2),1);
        List<Map<String,Object>> rows=new ArrayList<>();
        if(e.operation()==ChangeEvent.Operation.UPDATE) {
            if(e.before()==null)throw new IllegalArgumentException("A ClickHouse update requires its old key image");
            Map<String,Object> oldKey=new LinkedHashMap<>();
            for(String name:e.key().keySet()) {
                if(!e.before().containsKey(name)||e.before().get(name)==null)throw new IllegalArgumentException("ClickHouse old key is unavailable");
                oldKey.put(name,e.before().get(name));
            }
            if(!EventCodec.canonicalJson(oldKey).equals(EventCodec.canonicalJson(e.key())))
                rows.add(stateRow(e,oldKey,null,version-1,true,e.id()+":old-key"));
        }
        rows.add(stateRow(e,e.key(),e.after(),version,e.operation()==ChangeEvent.Operation.DELETE,e.id()));
        return rows;
    }
    private static Map<String,Object> stateRow(ChangeEvent e,Map<String,Object> key,Map<String,Object> payload,long version,boolean deleted,String eventId) {
        Map<String,Object> row=new LinkedHashMap<>();row.put("entity_key",EventCodec.canonicalJson(key));row.put("version",Long.toString(version));
        row.put("is_deleted",deleted?1:0);row.put("event_id",eventId);row.put("payload",payload==null?"{}":EventCodec.json(payload));
        row.put("source_position",e.position());row.put("source_time_ms",e.sourceTimeMillis());return row;
    }
    @Override public void close(){http.close();}
}
